"""
Solana Payment Module
Handles SOL payments with automatic splitting via middleman wallet.
"""

import logging
import requests
import asyncio
import time
import json
import sqlite3
import base58
import os
import random
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, List

from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.signature import Signature
from solders.system_program import TransferParams, transfer
from solders.transaction import Transaction
from solders.message import Message
from solders.rpc.responses import GetLatestBlockhashResp
from solana.rpc.api import Client as SolanaClient
from solana.rpc.commitment import Confirmed

from utils import (
    get_db_connection, format_currency, LANGUAGES,
    send_message_with_retry, get_first_primary_admin_id
)

logger = logging.getLogger(__name__)

# Will be imported from utils after configuration
SOL_WALLET1_ADDRESS = None
SOL_WALLET2_ADDRESS = None
SOL_MIDDLEMAN_ADDRESS = None
SOL_MIDDLEMAN_KEYPAIR = None
SOLSCAN_API_URL = None
SOLSCAN_API_KEY = None
SOL_CHECK_INTERVAL = 30
# Use custom RPC URL if provided, otherwise use public endpoint
# Handle empty strings by using 'or' operator
SOLANA_RPC_URL = os.environ.get("SOLANA_RPC_URL") or "https://api.mainnet-beta.solana.com"

# SOL to EUR conversion rate cache
sol_price_cache = {'price': Decimal('0'), 'timestamp': 0}
PRICE_CACHE_DURATION = 5400  # Cache price for 1.5 hours (90 minutes) to avoid CoinGecko rate limits

# Solana client
solana_client = None


def init_sol_config():
    """Initialize Solana configuration from utils."""
    global SOL_WALLET1_ADDRESS, SOL_WALLET2_ADDRESS, SOL_MIDDLEMAN_ADDRESS
    global SOL_MIDDLEMAN_KEYPAIR, SOLSCAN_API_URL, SOLSCAN_API_KEY
    global SOL_CHECK_INTERVAL, solana_client
    
    from utils import (
        SOL_WALLET1_ADDRESS as w1,
        SOL_WALLET2_ADDRESS as w2,
        SOL_MIDDLEMAN_ADDRESS as mm,
        SOL_MIDDLEMAN_PRIVATE_KEY as mm_key,
        SOLSCAN_API_URL as api_url,
        SOLSCAN_API_KEY as api_key,
        SOL_CHECK_INTERVAL as check_interval
    )
    
    SOL_WALLET1_ADDRESS = w1
    SOL_WALLET2_ADDRESS = w2
    SOL_MIDDLEMAN_ADDRESS = mm
    SOLSCAN_API_URL = api_url
    SOLSCAN_API_KEY = api_key
    SOL_CHECK_INTERVAL = check_interval
    
    # Initialize middleman keypair from private key (optional - only needed for split payments)
    if mm_key:
        try:
            private_key_bytes = base58.b58decode(mm_key)
            SOL_MIDDLEMAN_KEYPAIR = Keypair.from_bytes(private_key_bytes)
            logger.info(f"✅ Middleman keypair initialized: {str(SOL_MIDDLEMAN_KEYPAIR.pubkey())[:8]}...")
        except Exception as e:
            logger.error(f"Failed to initialize middleman keypair: {e}")
            logger.warning("⚠️ Middleman wallet not available. Split payments will fail!")
            SOL_MIDDLEMAN_KEYPAIR = None
    else:
        logger.warning("⚠️ SOL_MIDDLEMAN_PRIVATE_KEY not set. Split payments will not work!")
    
    # Initialize Solana RPC client
    global SOLANA_RPC_URL, solana_client  # Must declare as global to modify!
    
    # Validate RPC URL format
    if not SOLANA_RPC_URL.startswith(('http://', 'https://')):
        logger.error(f"❌ Invalid SOLANA_RPC_URL: '{SOLANA_RPC_URL}' - must start with http:// or https://")
        logger.warning("Using default RPC URL: https://api.mainnet-beta.solana.com")
        SOLANA_RPC_URL = "https://api.mainnet-beta.solana.com"
    
    solana_client = SolanaClient(SOLANA_RPC_URL)
    logger.info(f"✅ Solana client initialized: {SOLANA_RPC_URL}")


async def get_sol_price_eur() -> Optional[Decimal]:
    """Get current SOL price in EUR from CoinGecko API."""
    global sol_price_cache
    
    # Return cached price if still valid
    if time.time() - sol_price_cache['timestamp'] < PRICE_CACHE_DURATION:
        if sol_price_cache['price'] > Decimal('0'):
            return sol_price_cache['price']
    
    try:
        def fetch_price():
            response = requests.get(
                'https://api.coingecko.com/api/v3/simple/price',
                params={'ids': 'solana', 'vs_currencies': 'eur'},
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        
        data = await asyncio.to_thread(fetch_price)
        price = Decimal(str(data['solana']['eur']))
        
        # Update cache
        sol_price_cache['price'] = price
        sol_price_cache['timestamp'] = time.time()
        
        logger.info(f"💶 SOL price: {price:.2f} EUR")
        return price
        
    except requests.exceptions.HTTPError as e:
        if '429' in str(e):
            logger.warning(f"CoinGecko rate limit hit. Using cached or default price.")
        else:
            logger.error(f"HTTP error fetching SOL price: {e}")
        # Return cached price even if expired
        if sol_price_cache['price'] > Decimal('0'):
            logger.info(f"Using cached SOL price: {sol_price_cache['price']:.2f} EUR (age: {int(time.time() - sol_price_cache['timestamp'])}s)")
            return sol_price_cache['price']
        # Last resort: use approximate default price
        default_price = Decimal('135.0')  # Approximate SOL price
        logger.warning(f"No cache available. Using default SOL price: {default_price:.2f} EUR")
        sol_price_cache['price'] = default_price  # Cache it for next time
        sol_price_cache['timestamp'] = time.time()
        return default_price
    except Exception as e:
        logger.error(f"Error fetching SOL price: {e}")
        # Return cached price even if expired, better than nothing
        if sol_price_cache['price'] > Decimal('0'):
            logger.warning(f"Using expired cached SOL price: {sol_price_cache['price']:.2f} EUR")
            return sol_price_cache['price']
        # Last resort: use approximate default price
        default_price = Decimal('135.0')
        logger.warning(f"Using default SOL price: {default_price:.2f} EUR")
        sol_price_cache['price'] = default_price
        sol_price_cache['timestamp'] = time.time()
        return default_price


def determine_payment_wallet(basket_snapshot: list) -> str:
    """
    Determine which wallet should receive payment based on basket items.
    
    Rules:
    - If all items use same wallet, use that wallet
    - If any item is split, use middleman (will forward automatically)
    - If mixed wallets, use middleman (safer, can be manually distributed)
    """
    if not basket_snapshot:
        return 'wallet1'
    
    wallets = set()
    has_split = False
    
    for item in basket_snapshot:
        payout_wallet = item.get('payout_wallet', 'wallet1')
        if payout_wallet == 'split':
            has_split = True
        wallets.add(payout_wallet)
    
    # If any item is split, use middleman
    if has_split:
        return 'middleman'
    
    # If all items use same wallet, use that wallet directly
    if len(wallets) == 1:
        return wallets.pop()
    
    # Mixed wallets, use middleman for safety
    return 'middleman'


async def create_sol_payment(
    user_id: int,
    basket_snapshot: list,
    total_eur: Decimal,
    discount_code: Optional[str] = None
) -> dict:
    """
    Create a SOL payment request.
    Determines which wallet(s) should receive payment based on basket items.
    
    Returns:
        dict with payment details or error
    """
    try:
        # Get SOL price
        sol_price = await get_sol_price_eur()
        if not sol_price or sol_price <= Decimal('0'):
            logger.error("Failed to fetch SOL price")
            return {'error': 'price_fetch_failed'}
        
        # Calculate SOL amount needed (add 1% buffer for price fluctuation)
        sol_amount_base = (total_eur / sol_price).quantize(Decimal('0.000001'), rounding=ROUND_UP)
        sol_amount = sol_amount_base * Decimal('1.01')  # 1% buffer
        sol_amount = sol_amount.quantize(Decimal('0.000001'), rounding=ROUND_UP)
        
        # Add random offset to make each payment unique (prevents collision when multiple users buy same item)
        # Offset range: 0.000001 to 0.000099 SOL (~$0.0001 to $0.01)
        random_offset = Decimal(str(random.randint(1, 99))) / Decimal('1000000')
        sol_amount = sol_amount + random_offset
        logger.debug(f"Added random offset: +{random_offset:.6f} SOL (final: {sol_amount:.6f} SOL)")
        
        # Minimum SOL amount (0.01 SOL to avoid dust)
        min_sol = Decimal('0.01')
        if sol_amount < min_sol:
            return {
                'error': 'amount_too_low',
                'min_sol': float(min_sol),
                'min_eur': float(min_sol * sol_price)
            }
        
        # Determine which wallet should receive payment
        target_wallet = determine_payment_wallet(basket_snapshot)
        logger.info(f"💳 Determined payment destination for user {user_id}: {target_wallet}")
        logger.debug(f"Basket payout_wallet values: {[item.get('payout_wallet', 'N/A') for item in basket_snapshot]}")
        
        # Generate unique payment ID
        payment_id = f"SOL_{user_id}_{int(time.time())}_{hex(int(time.time() * 1000000))[-6:]}"
        
        # Store pending payment in database
        conn = None
        try:
            conn = get_db_connection()
            c = conn.cursor()
            
            now = datetime.now(timezone.utc)
            expires = now + timedelta(minutes=20)  # 20 minute expiry
            
            c.execute("""
                INSERT INTO pending_sol_payments 
                (payment_id, user_id, expected_sol_amount, expected_wallet, 
                 basket_snapshot, discount_code, created_at, expires_at, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')
            """, (
                payment_id,
                user_id,
                float(sol_amount),
                target_wallet,
                json.dumps(basket_snapshot),
                discount_code,
                now.isoformat(),
                expires.isoformat()
            ))
            
            conn.commit()
            logger.info(f"✅ Created SOL payment {payment_id}: {sol_amount} SOL (~{total_eur} EUR) to {target_wallet}")
            
        except sqlite3.Error as e:
            logger.error(f"Database error creating SOL payment: {e}")
            return {'error': 'database_error'}
        finally:
            if conn:
                conn.close()
        
        # Get wallet address to display
        if target_wallet == 'wallet1':
            wallet_address = SOL_WALLET1_ADDRESS
        elif target_wallet == 'wallet2':
            wallet_address = SOL_WALLET2_ADDRESS
        else:  # middleman
            wallet_address = SOL_MIDDLEMAN_ADDRESS
        
        return {
            'payment_id': payment_id,
            'sol_amount': float(sol_amount),
            'sol_price_eur': float(sol_price),
            'total_eur': float(total_eur),
            'wallet_address': wallet_address,
            'wallet_name': target_wallet,
            'expires_at': expires.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error creating SOL payment: {e}", exc_info=True)
        return {'error': 'internal_error'}


async def retry_rpc_call(func, max_retries=3, base_delay=1.0):
    """Retry RPC calls with exponential backoff for 429 errors."""
    for attempt in range(max_retries):
        try:
            return await func()
        except Exception as e:
            if '429' in str(e) and attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)  # Exponential backoff
                logger.warning(f"⏳ RPC rate limit hit, retrying in {delay}s (attempt {attempt + 1}/{max_retries})")
                await asyncio.sleep(delay)
            else:
                raise
    return None


async def check_wallet_transactions(wallet_address: str, limit: int = 20) -> List[Dict]:
    """
    Check recent transactions for a Solana wallet using Solana RPC.
    
    Returns:
        List of transaction dictionaries with incoming transfers
    """
    global solana_client
    
    if not solana_client:
        logger.error("❌ Solana client not initialized! Call init_sol_config() first.")
        return []
    
    try:
        logger.debug(f"Fetching signatures for {wallet_address[:8]}...")
        
        # First, verify the RPC is working by checking balance
        try:
            pubkey = Pubkey.from_string(wallet_address)
            balance_response = solana_client.get_balance(pubkey)
            if balance_response and balance_response.value is not None:
                balance_sol = Decimal(balance_response.value) / Decimal('1000000000')
                logger.info(f"💰 Wallet {wallet_address[:8]}... balance: {balance_sol:.6f} SOL")
            else:
                logger.warning(f"⚠️ Could not fetch balance for {wallet_address[:8]}...")
        except Exception as balance_err:
            logger.error(f"Error fetching balance: {balance_err}", exc_info=True)
        
        def fetch_signatures():
            # Get recent transaction signatures for this address
            pubkey = Pubkey.from_string(wallet_address)
            response = solana_client.get_signatures_for_address(
                pubkey,
                limit=limit,
                commitment=Confirmed
            )
            return response
        
        sig_response = await asyncio.to_thread(fetch_signatures)
        
        if not sig_response:
            logger.error(f"❌ NULL response from Solana RPC for {wallet_address[:8]}...")
            return []
        
        if not sig_response.value:
            logger.warning(f"⚠️ Empty signature list from Solana RPC for {wallet_address[:8]}... (wallet might be new or RPC issue)")
            return []
        
        logger.info(f"✅ Found {len(sig_response.value)} signature(s) for {wallet_address[:8]}...")
        
        transactions = []
        
        # Process each signature to get transaction details
        processed_count = 0
        VERBOSE_LIMIT = 3  # Only log details for first 3 transactions
        
        for sig_info in sig_response.value:
            signature = str(sig_info.signature)
            block_time = sig_info.block_time
            processed_count += 1
            verbose = processed_count <= VERBOSE_LIMIT
            
            if verbose:
                logger.info(f"Processing TX #{processed_count}/{len(sig_response.value)}: {signature[:16]}...")
            
            # Skip if transaction failed
            if sig_info.err:
                if verbose:
                    logger.info(f"  ⏭️ Skipping failed TX")
                continue
            
            try:
                # Fetch full transaction details with retry logic
                async def fetch_transaction_with_retry():
                    def fetch_transaction():
                        # Convert string signature to Signature object
                        sig_obj = Signature.from_string(signature)
                        return solana_client.get_transaction(
                            sig_obj,
                            encoding="jsonParsed",
                            commitment=Confirmed,
                            max_supported_transaction_version=0
                        )
                    return await asyncio.to_thread(fetch_transaction)
                
                tx_response = await retry_rpc_call(fetch_transaction_with_retry)
                
                # Add small delay to avoid RPC rate limits
                await asyncio.sleep(0.1)  # 100ms delay between requests
                
                if not tx_response or not tx_response.value:
                    if verbose:
                        logger.info(f"  ❌ No transaction data returned")
                    continue
                
                tx_data = tx_response.value
                
                # Parse transaction to find SOL transfers to our wallet
                if not (tx_data.transaction and tx_data.transaction.meta):
                    if verbose:
                        logger.info(f"  ❌ Missing transaction or meta")
                    continue
                
                meta = tx_data.transaction.meta
                
                # Check pre and post balances to find incoming SOL
                if not hasattr(tx_data.transaction.transaction, 'message'):
                    if verbose:
                        logger.info(f"  ❌ No message attribute")
                    continue
                
                message = tx_data.transaction.transaction.message
                account_keys = message.account_keys
                
                if verbose:
                    logger.info(f"  📋 Account keys: {len(account_keys)}, Pre/Post balances: {len(meta.pre_balances) if meta.pre_balances else 0}/{len(meta.post_balances) if meta.post_balances else 0}")
                
                # Find our wallet's index in the account keys
                our_index = None
                for idx, key in enumerate(account_keys):
                    key_str = str(key.pubkey) if hasattr(key, 'pubkey') else str(key)
                    if key_str == wallet_address:
                        our_index = idx
                        if verbose:
                            logger.info(f"  ✅ Found our wallet at index {idx}")
                        break
                
                if our_index is None:
                    if verbose:
                        logger.info(f"  ❌ Our wallet NOT in account keys - skipping")
                    continue
                
                if not (meta.pre_balances and meta.post_balances):
                    if verbose:
                        logger.info(f"  ❌ Missing pre or post balances")
                    continue
                
                if our_index >= len(meta.pre_balances) or our_index >= len(meta.post_balances):
                    if verbose:
                        logger.info(f"  ❌ Index {our_index} out of range")
                    continue
                
                pre_balance = meta.pre_balances[our_index]
                post_balance = meta.post_balances[our_index]
                
                pre_sol = Decimal(pre_balance) / Decimal('1000000000')
                post_sol = Decimal(post_balance) / Decimal('1000000000')
                
                if verbose:
                    logger.info(f"  💸 Balance: {pre_sol:.6f} → {post_sol:.6f} SOL")
                
                # Check if this is an incoming transfer (balance increased)
                if post_balance > pre_balance:
                    lamports_received = post_balance - pre_balance
                    sol_amount = Decimal(lamports_received) / Decimal('1000000000')
                    
                    logger.info(f"✅ INCOMING TX: {signature[:16]}... +{sol_amount:.6f} SOL")
                    
                    transactions.append({
                        'signature': signature,
                        'timestamp': block_time,
                        'amount_sol': sol_amount,
                        'confirmed': True
                    })
                else:
                    if verbose:
                        logger.info(f"  ⬇️ Not incoming (balance decreased or unchanged)")
            
            except Exception as tx_error:
                logger.warning(f"⚠️ Error processing TX {signature[:16]}...: {tx_error}")
                continue
        
        logger.info(f"📊 Processed {processed_count} transactions, found {len(transactions)} incoming")
        return transactions
        
    except Exception as e:
        logger.error(f"Error fetching wallet transactions for {wallet_address[:8]}...: {e}", exc_info=True)
        return []


async def verify_sol_transaction(signature: str) -> Optional[Dict]:
    """
    Verify a specific transaction by signature using Solana RPC.
    
    Returns:
        Transaction details if found and confirmed, None otherwise
    """
    global solana_client
    
    if not solana_client:
        logger.error("❌ Solana client not initialized!")
        return None
    
    try:
        def fetch_transaction():
            # Convert string signature to Signature object
            sig_obj = Signature.from_string(signature)
            response = solana_client.get_transaction(
                sig_obj,
                encoding="json",
                commitment=Confirmed,
                max_supported_transaction_version=0
            )
            return response
        
        response = await asyncio.to_thread(fetch_transaction)
        
        if response and response.value:
            tx_data = response.value
            # Check if transaction succeeded
            if tx_data.transaction.meta and tx_data.transaction.meta.err is None:
                return {
                    'signature': signature,
                    'confirmed': True,
                    'block_time': tx_data.block_time,
                    'slot': tx_data.slot
                }
        
        return None
        
    except Exception as e:
        logger.error(f"Error verifying transaction {signature}: {e}")
        return None


async def forward_split_payment(
    payment_id: str,
    source_signature: str,
    total_sol_amount: Decimal
) -> Dict[str, bool]:
    """
    Forward payment from middleman wallet to final wallets (20% to W1, 80% to W2).
    
    Returns:
        Dict with success status for each wallet
    """
    logger.info(f"🔄 Starting split forward for payment {payment_id}: {total_sol_amount} SOL")
    
    # Calculate split amounts (reserve a bit for fees)
    fee_reserve = Decimal('0.00002')  # Reserve for 2 transaction fees
    distributable = total_sol_amount - fee_reserve
    
    amount_wallet1 = (distributable * Decimal('0.20')).quantize(Decimal('0.000001'), rounding=ROUND_DOWN)
    amount_wallet2 = (distributable * Decimal('0.80')).quantize(Decimal('0.000001'), rounding=ROUND_DOWN)
    
    logger.info(f"💰 Split: {amount_wallet1} SOL → Asmenine (20%), {amount_wallet2} SOL → Kolegos (80%)")
    
    results = {'wallet1': False, 'wallet2': False}
    signatures = {'wallet1': None, 'wallet2': None}
    
    try:
        # Forward to Asmenine (20%)
        try:
            sig1 = await send_sol_transaction(
                from_keypair=SOL_MIDDLEMAN_KEYPAIR,
                to_address=SOL_WALLET1_ADDRESS,
                amount_sol=amount_wallet1
            )
            if sig1:
                logger.info(f"✅ Forwarded {amount_wallet1} SOL to Asmenine (wallet1): {sig1}")
                results['wallet1'] = True
                signatures['wallet1'] = sig1
            else:
                logger.error(f"❌ Failed to forward to Asmenine (wallet1)")
        except Exception as e:
            logger.error(f"❌ Error forwarding to Asmenine (wallet1): {e}")
        
        # Small delay between transactions
        await asyncio.sleep(1)
        
        # Forward to Kolegos (80%)
        try:
            sig2 = await send_sol_transaction(
                from_keypair=SOL_MIDDLEMAN_KEYPAIR,
                to_address=SOL_WALLET2_ADDRESS,
                amount_sol=amount_wallet2
            )
            if sig2:
                logger.info(f"✅ Forwarded {amount_wallet2} SOL to Kolegos (wallet2): {sig2}")
                results['wallet2'] = True
                signatures['wallet2'] = sig2
            else:
                logger.error(f"❌ Failed to forward to Kolegos (wallet2)")
        except Exception as e:
            logger.error(f"❌ Error forwarding to Kolegos (wallet2): {e}")
        
        # Log the forwarding
        conn = None
        try:
            conn = get_db_connection()
            c = conn.cursor()
            
            c.execute("""
                INSERT INTO sol_forwarding_log
                (payment_id, source_signature, wallet1_amount, wallet1_signature, 
                 wallet2_amount, wallet2_signature, forwarded_at, success)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                payment_id,
                source_signature,
                float(amount_wallet1),
                signatures['wallet1'],
                float(amount_wallet2),
                signatures['wallet2'],
                datetime.now(timezone.utc).isoformat(),
                1 if all(results.values()) else 0
            ))
            
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Database error logging forward: {e}")
        finally:
            if conn:
                conn.close()
        
        return results
        
    except Exception as e:
        logger.error(f"Error in forward_split_payment: {e}", exc_info=True)
        return results


async def send_sol_transaction(
    from_keypair: Keypair,
    to_address: str,
    amount_sol: Decimal
) -> Optional[str]:
    """
    Send SOL from one address to another.
    
    Returns:
        Transaction signature if successful, None otherwise
    """
    global solana_client
    
    if not solana_client:
        logger.error("❌ Solana client not initialized!")
        return None
    
    try:
        def send_tx():
            # Convert SOL to lamports
            lamports = int(amount_sol * Decimal('1000000000'))
            
            # Create transfer instruction
            transfer_ix = transfer(
                TransferParams(
                    from_pubkey=from_keypair.pubkey(),
                    to_pubkey=Pubkey.from_string(to_address),
                    lamports=lamports
                )
            )
            
            # Get recent blockhash
            blockhash_resp = solana_client.get_latest_blockhash()
            if not blockhash_resp or not blockhash_resp.value:
                logger.error("Failed to get recent blockhash")
                return None
            
            recent_blockhash = blockhash_resp.value.blockhash
            
            # Create transaction
            message = Message.new_with_blockhash(
                [transfer_ix],
                from_keypair.pubkey(),
                recent_blockhash
            )
            transaction = Transaction([from_keypair], message, recent_blockhash)
            
            # Send transaction (transaction already signed, don't pass keypair again)
            response = solana_client.send_transaction(
                transaction,
                opts={'skip_preflight': False, 'preflight_commitment': Confirmed}
            )
            
            if response and response.value:
                return str(response.value)
            
            return None
        
        signature = await asyncio.to_thread(send_tx)
        
        if signature:
            # Wait for confirmation
            await asyncio.sleep(2)
            confirmed = await verify_sol_transaction(signature)
            if confirmed:
                return signature
        
        return None
        
    except Exception as e:
        logger.error(f"Error sending SOL transaction: {e}", exc_info=True)
        return None


async def process_pending_sol_payments(context):
    """
    Background task to check for incoming SOL payments.
    Runs continuously to monitor pending payments.
    """
    try:
        logger.info("🔍 Starting SOL payment monitoring service...")
        
        # Initialize configuration
        init_sol_config()
        logger.info("✅ SOL configuration initialized successfully")
        
    except Exception as e:
        logger.critical(f"❌ CRITICAL: Failed to initialize SOL payment monitoring: {e}", exc_info=True)
        logger.critical("SOL payments will NOT be monitored automatically!")
        return
    
    logger.info(f"💰 Monitoring wallets: Asmenine={SOL_WALLET1_ADDRESS[:8]}..., Kolegos={SOL_WALLET2_ADDRESS[:8]}..., Middleman={SOL_MIDDLEMAN_ADDRESS[:8]}...")
    
    while True:
        try:
            await check_pending_payments(context)
        except Exception as e:
            logger.error(f"Error in payment monitoring loop: {e}", exc_info=True)
        
        # Wait before next check
        await asyncio.sleep(SOL_CHECK_INTERVAL)


async def check_pending_payments(context):
    """Check all pending SOL payments for confirmations."""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get all pending payments
        c.execute("""
            SELECT payment_id, user_id, expected_sol_amount, expected_wallet, 
                   basket_snapshot, discount_code, created_at, expires_at
            FROM pending_sol_payments
            WHERE status = 'pending'
        """)
        
        pending = c.fetchall()
        
        if not pending:
            logger.debug("No pending SOL payments to check")
            return
        
        logger.info(f"🔍 Checking {len(pending)} pending SOL payment(s)...")
        
        for payment in pending:
            payment_id = payment['payment_id']
            user_id = payment['user_id']
            expected_amount = Decimal(str(payment['expected_sol_amount']))
            expected_wallet = payment['expected_wallet']
            created_at = datetime.fromisoformat(payment['created_at'])
            expires_at = datetime.fromisoformat(payment['expires_at'])
            
            # Check if payment expired
            if datetime.now(timezone.utc) > expires_at:
                logger.info(f"⏱️ Payment {payment_id} expired")
                c.execute("""
                    UPDATE pending_sol_payments 
                    SET status = 'expired' 
                    WHERE payment_id = ?
                """, (payment_id,))
                conn.commit()
                
                # Unreserve basket items
                try:
                    basket_snapshot = json.loads(payment['basket_snapshot'])
                    from user import _unreserve_basket_items
                    await asyncio.to_thread(_unreserve_basket_items, basket_snapshot)
                    logger.info(f"♻️ Unreserved items for expired payment {payment_id}")
                except Exception as e:
                    logger.error(f"Error unreserving items: {e}")
                
                continue
            
            # Get wallet address to check
            if expected_wallet == 'wallet1':
                wallet_address = SOL_WALLET1_ADDRESS
            elif expected_wallet == 'wallet2':
                wallet_address = SOL_WALLET2_ADDRESS
            else:  # middleman
                wallet_address = SOL_MIDDLEMAN_ADDRESS
            
            logger.info(f"💳 Payment {payment_id}: expecting {expected_amount:.6f} SOL to {wallet_address[:8]}...")
            
            # Check recent transactions to this wallet
            transactions = await check_wallet_transactions(wallet_address, limit=20)
            logger.info(f"📊 Found {len(transactions)} transaction(s) for wallet {wallet_address[:8]}...")
            
            # Look for matching transaction (allow 1% tolerance for price fluctuation/fees)
            tolerance = expected_amount * Decimal('0.01')
            min_amount = expected_amount - tolerance
            max_amount = expected_amount + tolerance
            logger.debug(f"Tolerance range: {min_amount:.6f} to {max_amount:.6f} SOL")
            
            # Only consider recent transactions (within 30 minutes of payment creation)
            recent_cutoff = created_at - timedelta(minutes=30)
            
            for tx in transactions:
                tx_amount = tx['amount_sol']
                tx_signature = tx['signature']
                tx_timestamp = tx.get('timestamp')
                
                # Skip transactions that are too old (before payment was created minus 30 min buffer)
                if tx_timestamp:
                    tx_datetime = datetime.fromtimestamp(tx_timestamp, tz=timezone.utc)
                    if tx_datetime < recent_cutoff:
                        logger.debug(f"Skipping old transaction {tx_signature[:16]}... from {tx_datetime.isoformat()}")
                        continue
                
                logger.debug(f"Checking TX {tx_signature[:16]}...: {tx_amount:.6f} SOL")
                
                # Check if transaction matches expected amount (within tolerance, both upper AND lower bounds)
                if min_amount <= tx_amount <= max_amount:
                    logger.info(f"💰 Found matching amount! TX: {tx_signature[:16]}... = {tx_amount:.6f} SOL (expected: {expected_amount:.6f} ±1%)")
                    # Check if we already processed this transaction
                    c.execute("""
                        SELECT signature FROM processed_sol_transactions 
                        WHERE signature = ?
                    """, (tx_signature,))
                    
                    if c.fetchone():
                        logger.debug(f"Transaction {tx_signature} already processed")
                        continue
                    
                    # Also verify this transaction isn't already assigned to another payment
                    c.execute("""
                        SELECT payment_id FROM processed_sol_transactions 
                        WHERE signature = ? AND payment_id != ?
                    """, (tx_signature, payment_id))
                    
                    if c.fetchone():
                        logger.warning(f"Transaction {tx_signature} already used for different payment")
                        continue
                    
                    # Transaction is already confirmed (we only get confirmed txs from check_wallet_transactions)
                    # The 'confirmed' field in tx dict indicates it passed all checks
                    if not tx.get('confirmed'):
                        logger.warning(f"Transaction {tx_signature} not confirmed")
                        continue
                    
                    logger.info(f"✅ Payment {payment_id} confirmed! TX: {tx_signature}")
                    
                    # Start atomic transaction for race condition safety
                    try:
                        c.execute("BEGIN IMMEDIATE")
                        
                        # Re-check if transaction was processed (race condition protection)
                        c.execute("""
                            SELECT signature FROM processed_sol_transactions 
                            WHERE signature = ?
                        """, (tx_signature,))
                        
                        if c.fetchone():
                            logger.warning(f"Transaction {tx_signature} was processed by another thread, skipping")
                            c.execute("ROLLBACK")
                            continue
                        
                        # If payment went to middleman, forward it BEFORE marking as processed
                        forward_success = True
                        if expected_wallet == 'middleman':
                            logger.info(f"🔄 Payment to middleman, initiating split forward...")
                            # Rollback transaction temporarily for forwarding
                            c.execute("ROLLBACK")
                            
                            forward_results = await forward_split_payment(
                                payment_id,
                                tx_signature,
                                tx_amount
                            )
                            
                            forward_success = all(forward_results.values())
                            
                            if not forward_success:
                                logger.error(f"❌ Split forward failed: {forward_results}")
                                # Alert admin
                                if get_first_primary_admin_id():
                                    admin_msg = (
                                        f"⚠️ SPLIT PAYMENT FORWARD FAILED\n"
                                        f"Payment: {payment_id}\n"
                                        f"TX: {tx_signature}\n"
                                        f"Amount: {tx_amount} SOL\n"
                                        f"Asmenine (wallet1): {'✅' if forward_results.get('wallet1') else '❌'}\n"
                                        f"Kolegos (wallet2): {'✅' if forward_results.get('wallet2') else '❌'}\n"
                                        f"Manual intervention required!"
                                    )
                                    try:
                                        await send_message_with_retry(
                                            context.bot,
                                            get_first_primary_admin_id(),
                                            admin_msg,
                                            parse_mode=None
                                        )
                                    except Exception:
                                        pass
                                # Don't process payment if forward failed
                                continue
                            
                            # Restart atomic transaction after successful forwarding
                            c.execute("BEGIN IMMEDIATE")
                            
                            # Re-check again after forwarding (another race condition check)
                            c.execute("""
                                SELECT signature FROM processed_sol_transactions 
                                WHERE signature = ?
                            """, (tx_signature,))
                            
                            if c.fetchone():
                                logger.warning(f"Transaction {tx_signature} was processed during forwarding, skipping")
                                c.execute("ROLLBACK")
                                continue
                        
                        # Mark transaction as processed (atomic with payment confirmation)
                        c.execute("""
                            INSERT INTO processed_sol_transactions 
                            (signature, payment_id, processed_at, amount)
                            VALUES (?, ?, ?, ?)
                        """, (
                            tx_signature,
                            payment_id,
                            datetime.now(timezone.utc).isoformat(),
                            float(tx_amount)
                        ))
                        
                        # Mark payment as confirmed
                        c.execute("""
                            UPDATE pending_sol_payments 
                            SET status = 'confirmed', transaction_signature = ?
                            WHERE payment_id = ?
                        """, (tx_signature, payment_id))
                        
                        # Commit atomic transaction
                        conn.commit()
                        logger.info(f"✅ Transaction {tx_signature} and payment {payment_id} atomically processed")
                        
                    except Exception as atomic_error:
                        logger.error(f"Error in atomic transaction processing: {atomic_error}")
                        try:
                            c.execute("ROLLBACK")
                        except:
                            pass
                        continue
                    
                    # Process the purchase (outside atomic transaction)
                    basket_snapshot = json.loads(payment['basket_snapshot'])
                    discount_code = payment['discount_code']
                    
                    await finalize_sol_purchase(
                        user_id=user_id,
                        basket_snapshot=basket_snapshot,
                        discount_code=discount_code,
                        payment_id=payment_id,
                        transaction_signature=tx_signature,
                        context=context
                    )
                    
                    break  # Payment processed, move to next pending payment
        
    except sqlite3.Error as e:
        logger.error(f"Database error checking payments: {e}")
    except Exception as e:
        logger.error(f"Error checking pending payments: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()


async def finalize_sol_purchase(user_id, basket_snapshot, discount_code, payment_id, transaction_signature, context):
    """Finalize purchase after SOL payment confirmed."""
    logger.info(f"🎉 Finalizing SOL purchase for user {user_id}, payment {payment_id}")
    
    # Import here to avoid circular imports
    from payment import _finalize_purchase
    
    # Call the shared finalization logic
    success = await _finalize_purchase(user_id, basket_snapshot, discount_code, context)
    
    if success:
        logger.info(f"✅ SOL purchase completed successfully for user {user_id}")
        
        # Send confirmation to user with transaction link
        # Payment confirmation message removed per user request
        # Product will be delivered directly without separate confirmation message
        pass
    else:
        logger.error(f"❌ Failed to finalize SOL purchase for user {user_id}")
        
        # Alert admin
        if get_first_primary_admin_id():
            admin_msg = (
                f"⚠️ PURCHASE FINALIZATION FAILED\n"
                f"Payment: {payment_id}\n"
                f"User: {user_id}\n"
                f"TX: {transaction_signature}\n"
                f"Payment confirmed but purchase processing failed!"
            )
            try:
                await send_message_with_retry(
                    context.bot,
                    get_first_primary_admin_id(),
                    admin_msg,
                    parse_mode=None
                )
            except Exception:
                pass


async def cancel_sol_payment(payment_id: str) -> bool:
    """Cancel a pending SOL payment and unreserve items."""
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        
        # Get payment details
        c.execute("""
            SELECT basket_snapshot, status FROM pending_sol_payments
            WHERE payment_id = ?
        """, (payment_id,))
        
        payment = c.fetchone()
        
        if not payment:
            logger.warning(f"Payment {payment_id} not found for cancellation")
            return False
        
        if payment['status'] != 'pending':
            logger.warning(f"Payment {payment_id} status is {payment['status']}, cannot cancel")
            return False
        
        # Mark as cancelled
        c.execute("""
            UPDATE pending_sol_payments
            SET status = 'cancelled'
            WHERE payment_id = ?
        """, (payment_id,))
        
        conn.commit()
        
        # Unreserve items
        try:
            basket_snapshot = json.loads(payment['basket_snapshot'])
            from user import _unreserve_basket_items
            await asyncio.to_thread(_unreserve_basket_items, basket_snapshot)
            logger.info(f"✅ Cancelled payment {payment_id} and unreserved items")
            return True
        except Exception as e:
            logger.error(f"Error unreserving items during cancellation: {e}")
            return False
        
    except sqlite3.Error as e:
        logger.error(f"Database error cancelling payment: {e}")
        return False
    finally:
        if conn:
            conn.close()


# Export key functions
__all__ = [
    'init_sol_config',
    'get_sol_price_eur',
    'create_sol_payment',
    'process_pending_sol_payments',
    'cancel_sol_payment'
]

