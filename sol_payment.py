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
from solana.rpc.types import TxOpts

from utils import (
    get_db_connection, format_currency, LANGUAGES,
    send_message_with_retry, get_first_primary_admin_id
)

logger = logging.getLogger(__name__)

# Global lock to prevent concurrent split forwards (race conditions)
_split_forward_lock = asyncio.Lock()

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
    logger.debug(f"🔍 [WALLET DETERMINATION] Analyzing {len(basket_snapshot) if basket_snapshot else 0} items")
    
    if not basket_snapshot:
        logger.warning("  ⚠️ Empty basket snapshot, defaulting to wallet1")
        return 'wallet1'
    
    wallets = set()
    has_split = False
    
    for idx, item in enumerate(basket_snapshot):
        payout_wallet = item.get('payout_wallet', 'wallet1')
        product_id = item.get('product_id', 'unknown')
        logger.debug(f"  Item {idx + 1}/{len(basket_snapshot)}: product_id={product_id}, payout_wallet='{payout_wallet}'")
        
        if payout_wallet == 'split':
            has_split = True
            logger.debug(f"    ↳ Split detected!")
        wallets.add(payout_wallet)
    
    # If any item is split, use middleman
    if has_split:
        logger.info(f"✅ [WALLET DETERMINATION] → middleman (split payment required)")
        return 'middleman'
    
    # If all items use same wallet, use that wallet directly
    if len(wallets) == 1:
        target = wallets.pop()
        logger.info(f"✅ [WALLET DETERMINATION] → {target} (all items use same wallet)")
        return target
    
    # Mixed wallets, use middleman for safety
    logger.info(f"✅ [WALLET DETERMINATION] → middleman (mixed wallets: {wallets})")
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
    logger.info(f"💰 [CREATE SOL PAYMENT] User {user_id}: Starting payment creation for {total_eur} EUR")
    logger.debug(f"  Basket: {len(basket_snapshot)} items, Discount: {discount_code}")
    
    try:
        # Get SOL price
        logger.debug("  Step 1: Fetching SOL price...")
        sol_price = await get_sol_price_eur()
        if not sol_price or sol_price <= Decimal('0'):
            logger.error("  ❌ Failed to fetch SOL price")
            return {'error': 'price_fetch_failed'}
        logger.debug(f"  ✅ SOL price: {sol_price:.2f} EUR")
        
        # Calculate SOL amount needed (add 1% buffer for price fluctuation)
        logger.debug("  Step 2: Calculating SOL amount...")
        sol_amount_base = (total_eur / sol_price).quantize(Decimal('0.000001'), rounding=ROUND_UP)
        logger.debug(f"    Base amount: {sol_amount_base:.6f} SOL")
        sol_amount = sol_amount_base * Decimal('1.01')  # 1% buffer
        sol_amount = sol_amount.quantize(Decimal('0.000001'), rounding=ROUND_UP)
        logger.debug(f"    With 1% buffer: {sol_amount:.6f} SOL")
        
        # Add random offset to make each payment unique (prevents collision when multiple users buy same item)
        # Offset range: 0.000001 to 0.009999 SOL (9999 possible values for better uniqueness)
        random_offset = Decimal(str(random.randint(1, 9999))) / Decimal('1000000')
        sol_amount = sol_amount + random_offset
        logger.info(f"  ✅ Final amount: {sol_amount:.6f} SOL (base: {sol_amount_base:.6f}, buffer: +1%, offset: +{random_offset:.6f})")
        
        # Minimum SOL amount (0.01 SOL to avoid dust)
        min_sol = Decimal('0.01')
        if sol_amount < min_sol:
            logger.warning(f"  ❌ Amount {sol_amount:.6f} SOL below minimum {min_sol} SOL")
            return {
                'error': 'amount_too_low',
                'min_sol': float(min_sol),
                'min_eur': float(min_sol * sol_price)
            }
        
        # Determine which wallet should receive payment
        logger.debug("  Step 3: Determining payment wallet...")
        target_wallet = determine_payment_wallet(basket_snapshot)
        logger.info(f"  💳 Payment destination: {target_wallet}")
        logger.debug(f"  Basket payout_wallet values: {[item.get('payout_wallet', 'N/A') for item in basket_snapshot]}")
        
        # Generate unique payment ID
        payment_id = f"SOL_{user_id}_{int(time.time())}_{hex(int(time.time() * 1000000))[-6:]}"
        logger.debug(f"  Generated payment_id: {payment_id}")
        
        # Store pending payment in database
        logger.debug("  Step 4: Storing payment in database...")
        conn = None
        try:
            conn = get_db_connection()
            c = conn.cursor()
            
            now = datetime.now(timezone.utc)
            expires = now + timedelta(minutes=20)  # 20 minute expiry
            
            logger.debug(f"    Inserting: payment_id={payment_id}, amount={sol_amount:.6f} SOL, wallet={target_wallet}")
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
            logger.info(f"✅ [CREATE SOL PAYMENT] Payment {payment_id} created: {sol_amount:.6f} SOL (~{total_eur} EUR) → {target_wallet}")
            
        except sqlite3.Error as e:
            logger.error(f"Database error creating SOL payment: {e}")
            return {'error': 'database_error'}
        finally:
            if conn:
                conn.close()
        
        # Get wallet address to display
        logger.debug("  Step 5: Resolving wallet address...")
        if target_wallet == 'wallet1':
            wallet_address = SOL_WALLET1_ADDRESS
            logger.debug(f"    wallet1 → {wallet_address[:8]}...")
        elif target_wallet == 'wallet2':
            wallet_address = SOL_WALLET2_ADDRESS
            logger.debug(f"    wallet2 → {wallet_address[:8]}...")
        else:  # middleman
            wallet_address = SOL_MIDDLEMAN_ADDRESS
            logger.debug(f"    middleman → {wallet_address[:8]}...")
        
        logger.info(f"🎉 [CREATE SOL PAYMENT] User {user_id}: Payment ready! {sol_amount:.6f} SOL to {wallet_address[:8]}...")
        
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
    Uses a lock to prevent concurrent forwards from causing race conditions.
    
    Returns:
        Dict with success status for each wallet
    """
    # CRITICAL: Acquire lock to prevent concurrent forwards depleting the same balance
    async with _split_forward_lock:
        return await _forward_split_payment_locked(payment_id, total_sol_amount, source_signature)


async def _forward_split_payment_locked(payment_id: str, total_sol_amount: Decimal, source_signature: str) -> Dict[str, bool]:
    """Internal function that does the actual forwarding. Called within lock."""
    logger.info(f"🔄 [SPLIT FORWARD] Payment {payment_id}: Starting split forward (LOCK ACQUIRED)")
    logger.info(f"  Source TX: {source_signature[:16]}..., Amount: {total_sol_amount:.6f} SOL")
    
    # Solana transaction fees: Each transfer costs ~0.000005 SOL
    # The fee is deducted from the sender's balance IN ADDITION to the transfer amount
    # Solana rent-exempt minimum: ~0.00089088 SOL
    # Using conservative estimate to handle network congestion
    TX_FEE_ESTIMATE = Decimal('0.00001')  # Per transaction (2x typical for safety)
    TOTAL_FEES = TX_FEE_ESTIMATE * 2  # Two transactions (20% + 80%)
    MIN_RESERVE_BALANCE = Decimal('0.002')  # Permanent reserve: rent (~0.00089088) + buffer
    MIN_FORWARDABLE = Decimal('0.000010')  # Minimum to forward (prevent dust transfers)
    
    logger.debug(f"  Constants: TX_FEE={TX_FEE_ESTIMATE:.6f}, TOTAL_FEES={TOTAL_FEES:.6f}, MIN_RESERVE={MIN_RESERVE_BALANCE:.6f}")
    
    # Check middleman wallet balance and calculate how much we can safely forward
    logger.debug("  Step 1: Checking middleman wallet balance...")
    try:
        middleman_pubkey = Pubkey.from_string(SOL_MIDDLEMAN_ADDRESS)
        balance_response = solana_client.get_balance(middleman_pubkey)
        current_balance = Decimal(balance_response.value) / Decimal(1_000_000_000)
        logger.info(f"  💰 Current middleman balance: {current_balance:.6f} SOL")
        
        # Calculate maximum we can forward while keeping the reserve
        logger.debug("  Step 2: Calculating forwardable amount...")
        logger.debug(f"    Formula: max_forwardable = current_balance - MIN_RESERVE - TOTAL_FEES")
        logger.debug(f"    Formula: max_forwardable = {current_balance:.6f} - {MIN_RESERVE_BALANCE:.6f} - {TOTAL_FEES:.6f}")
        max_forwardable = current_balance - MIN_RESERVE_BALANCE - TOTAL_FEES
        logger.debug(f"    Result: max_forwardable = {max_forwardable:.6f} SOL")
        
        if max_forwardable <= 0:
            logger.error(f"  ❌ Middleman wallet balance too low to forward!")
            logger.error(f"     Current: {current_balance:.6f} SOL")
            logger.error(f"     Reserve needed: {MIN_RESERVE_BALANCE:.6f} SOL")
            logger.error(f"     Fees needed: {TOTAL_FEES:.6f} SOL")
            logger.error(f"     Shortfall: {abs(max_forwardable):.6f} SOL")
            logger.error(f"  ⚠️ URGENT: Fund middleman wallet with at least 0.005 SOL!")
            return {'wallet1': False, 'wallet2': False}
        
        # Determine how much to forward
        logger.debug("  Step 3: Determining forward amount...")
        if max_forwardable >= total_sol_amount:
            # IDEAL: We can forward the full payment amount
            forwardable = total_sol_amount
            logger.info(f"  ✅ Can forward FULL payment amount: {forwardable:.6f} SOL")
            logger.debug(f"     max_forwardable ({max_forwardable:.6f}) >= total_sol_amount ({total_sol_amount:.6f})")
        else:
            # FALLBACK: Forward only what we can while keeping reserve
            forwardable = max_forwardable
            shortfall = total_sol_amount - max_forwardable
            logger.warning(f"  ⚠️ Can only forward {forwardable:.6f} SOL (shortfall: {shortfall:.6f} SOL)")
            logger.warning(f"     Requested: {total_sol_amount:.6f} SOL")
            logger.warning(f"     Available: {max_forwardable:.6f} SOL (after reserve + fees)")
            logger.warning(f"     Reason: Must keep {MIN_RESERVE_BALANCE:.6f} SOL reserve + {TOTAL_FEES:.6f} SOL fees")
        
        # Split the forwardable amount 20/80
        logger.debug("  Step 4: Calculating split amounts...")
        amount_wallet1_raw = forwardable * Decimal('0.20')
        amount_wallet2_raw = forwardable * Decimal('0.80')
        amount_wallet1 = amount_wallet1_raw.quantize(Decimal('0.000001'), rounding=ROUND_DOWN)
        amount_wallet2 = amount_wallet2_raw.quantize(Decimal('0.000001'), rounding=ROUND_DOWN)
        
        logger.debug(f"    20% of {forwardable:.6f} = {amount_wallet1_raw:.6f} → {amount_wallet1:.6f} SOL (rounded down)")
        logger.debug(f"    80% of {forwardable:.6f} = {amount_wallet2_raw:.6f} → {amount_wallet2:.6f} SOL (rounded down)")
        logger.info(f"  💰 Split amounts:")
        logger.info(f"     Asmenine (20%): {amount_wallet1:.6f} SOL")
        logger.info(f"     Kolegos (80%):  {amount_wallet2:.6f} SOL")
        logger.info(f"     Total split:    {amount_wallet1 + amount_wallet2:.6f} SOL")
        
        predicted_balance = current_balance - forwardable - TOTAL_FEES
        logger.info(f"  📊 Balance prediction:")
        logger.info(f"     Before: {current_balance:.6f} SOL")
        logger.info(f"     After:  ~{predicted_balance:.6f} SOL (minus fees)")
        
        # CRITICAL: Block if predicted balance would go below reserve
        if predicted_balance < MIN_RESERVE_BALANCE:
            logger.error(f"  ❌ BLOCKED: Predicted balance ({predicted_balance:.6f}) below reserve ({MIN_RESERVE_BALANCE:.6f})!")
            logger.error(f"     This forward would drain the wallet below rent-exempt minimum")
            logger.error(f"     Forwardable amount must be reduced or wallet must be funded")
            logger.error(f"  ⚠️ URGENT: Fund middleman wallet with at least {MIN_RESERVE_BALANCE - predicted_balance + Decimal('0.001'):.6f} SOL!")
            return {'wallet1': False, 'wallet2': False}
        
        # Validate minimum forwardable amounts
        if amount_wallet1 < MIN_FORWARDABLE or amount_wallet2 < MIN_FORWARDABLE:
            logger.error(f"  ❌ BLOCKED: Split amounts below minimum forwardable ({MIN_FORWARDABLE:.6f} SOL)")
            logger.error(f"     Asmenine (20%): {amount_wallet1:.6f} SOL {'✅' if amount_wallet1 >= MIN_FORWARDABLE else '❌ TOO SMALL'}")
            logger.error(f"     Kolegos (80%):  {amount_wallet2:.6f} SOL {'✅' if amount_wallet2 >= MIN_FORWARDABLE else '❌ TOO SMALL'}")
            logger.error(f"     Payment amount ({total_sol_amount:.6f} SOL) too small for split forwarding")
            return {'wallet1': False, 'wallet2': False}
    
    except Exception as e:
        logger.error(f"❌ Failed to check middleman balance: {e}", exc_info=True)
        # FALLBACK: Can't check balance, so deduct small safety buffer from payment
        logger.warning(f"⚠️ FALLBACK: Can't verify balance, deducting safety buffer")
        safety_buffer = TOTAL_FEES + Decimal('0.001')  # Fees + small buffer for rent
        forwardable = total_sol_amount - safety_buffer
        
        if forwardable <= 0:
            logger.error(f"❌ Payment too small to forward with safety buffer!")
            return {'wallet1': False, 'wallet2': False}
        
        amount_wallet1 = (forwardable * Decimal('0.20')).quantize(Decimal('0.000001'), rounding=ROUND_DOWN)
        amount_wallet2 = (forwardable * Decimal('0.80')).quantize(Decimal('0.000001'), rounding=ROUND_DOWN)
        logger.info(f"💰 Split with safety buffer: {amount_wallet1} SOL → Asmenine, {amount_wallet2} SOL → Kolegos")
    
    results = {'wallet1': False, 'wallet2': False}
    signatures = {'wallet1': None, 'wallet2': None}
    
    try:
        # IMPORTANT: Forward to Kolegos (80%) FIRST - larger amount more likely to fail
        # If 80% fails, we don't send the 20%, preventing partial splits
        logger.info(f"  📤 [FORWARD 1/2] Sending {amount_wallet2:.6f} SOL to Kolegos (80%)...")
        logger.debug(f"     From: {SOL_MIDDLEMAN_ADDRESS[:8]}...")
        logger.debug(f"     To: {SOL_WALLET2_ADDRESS[:8]}...")
        try:
            sig2 = await send_sol_transaction(
                from_keypair=SOL_MIDDLEMAN_KEYPAIR,
                to_address=SOL_WALLET2_ADDRESS,
                amount_sol=amount_wallet2
            )
            if sig2:
                logger.info(f"  ✅ [FORWARD 1/2] Success! TX: {sig2[:16]}...")
                logger.info(f"     Kolegos received {amount_wallet2:.6f} SOL")
                results['wallet2'] = True
                signatures['wallet2'] = sig2
            else:
                logger.error(f"  ❌ [FORWARD 1/2] Failed - no signature returned")
                logger.error(f"     Kolegos (wallet2) forward failed, aborting split")
                # Don't proceed to wallet1 if wallet2 failed
                return results
        except Exception as e:
            logger.error(f"  ❌ [FORWARD 1/2] Exception: {e}", exc_info=True)
            logger.error(f"     Kolegos (wallet2) forward failed, aborting split")
            # Don't proceed to wallet1 if wallet2 failed
            return results
        
        # Small delay between transactions
        logger.debug(f"  ⏳ Waiting 1 second before second transfer...")
        await asyncio.sleep(1)
        
        # SAFETY CHECK: Verify wallet still has enough for second transfer + reserve
        try:
            logger.debug(f"  🔍 Rechecking middleman balance after first transfer...")
            balance_response_after = solana_client.get_balance(middleman_pubkey)
            balance_after_first = Decimal(balance_response_after.value) / Decimal(1_000_000_000)
            logger.debug(f"     Balance after 1st TX: {balance_after_first:.6f} SOL")
            
            required_for_second = amount_wallet1 + TX_FEE_ESTIMATE + MIN_RESERVE_BALANCE
            logger.debug(f"     Required for 2nd TX: {required_for_second:.6f} SOL (amount + fee + reserve)")
            
            if balance_after_first < required_for_second:
                logger.error(f"  ❌ [FORWARD 2/2] Insufficient balance after first transfer!")
                logger.error(f"     Current: {balance_after_first:.6f} SOL")
                logger.error(f"     Needed: {required_for_second:.6f} SOL")
                logger.error(f"     Shortfall: {required_for_second - balance_after_first:.6f} SOL")
                logger.error(f"     PARTIAL SPLIT: Only Kolegos (80%) received funds")
                # Don't attempt second transfer
                return results
            logger.debug(f"     ✅ Sufficient balance for second transfer")
        except Exception as balance_check_err:
            logger.warning(f"  ⚠️ Failed to recheck balance: {balance_check_err}")
            logger.warning(f"     Proceeding with second transfer anyway...")
        
        # Forward to Asmenine (20%) - only if Kolegos succeeded
        logger.info(f"  📤 [FORWARD 2/2] Sending {amount_wallet1:.6f} SOL to Asmenine (20%)...")
        logger.debug(f"     From: {SOL_MIDDLEMAN_ADDRESS[:8]}...")
        logger.debug(f"     To: {SOL_WALLET1_ADDRESS[:8]}...")
        try:
            sig1 = await send_sol_transaction(
                from_keypair=SOL_MIDDLEMAN_KEYPAIR,
                to_address=SOL_WALLET1_ADDRESS,
                amount_sol=amount_wallet1
            )
            if sig1:
                logger.info(f"  ✅ [FORWARD 2/2] Success! TX: {sig1[:16]}...")
                logger.info(f"     Asmenine received {amount_wallet1:.6f} SOL")
                results['wallet1'] = True
                signatures['wallet1'] = sig1
            else:
                logger.error(f"  ❌ [FORWARD 2/2] Failed - no signature returned")
                logger.error(f"     Asmenine (wallet1) forward failed!")
        except Exception as e:
            logger.error(f"  ❌ [FORWARD 2/2] Exception: {e}", exc_info=True)
            logger.error(f"     Asmenine (wallet1) forward failed!")
        
        # Log the forwarding
        logger.debug("  Step 5: Recording forward in database...")
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
            logger.debug("     ✅ Forward logged to database")
        except sqlite3.Error as e:
            logger.error(f"     ❌ Database error logging forward: {e}")
        finally:
            if conn:
                conn.close()
        
        # Final summary
        success_count = sum(1 for v in results.values() if v)
        if all(results.values()):
            logger.info(f"🎉 [SPLIT FORWARD] Payment {payment_id}: SUCCESS - Both transfers completed")
            logger.info(f"   Asmenine: {amount_wallet1:.6f} SOL ✅")
            logger.info(f"   Kolegos:  {amount_wallet2:.6f} SOL ✅")
        else:
            logger.error(f"❌ [SPLIT FORWARD] Payment {payment_id}: PARTIAL/FAILED - {success_count}/2 transfers completed")
            logger.error(f"   Asmenine: {amount_wallet1:.6f} SOL {'✅' if results['wallet1'] else '❌'}")
            logger.error(f"   Kolegos:  {amount_wallet2:.6f} SOL {'✅' if results['wallet2'] else '❌'}")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ [SPLIT FORWARD] Unexpected error: {e}", exc_info=True)
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
        logger.debug(f"     🔧 Converting {amount_sol} SOL to lamports...")
        lamports = int(amount_sol * Decimal('1000000000'))
        logger.debug(f"     🔧 Amount: {lamports} lamports")
        
        def send_tx():
            try:
                logger.debug(f"     🔧 Creating transfer instruction...")
                # Create transfer instruction
                transfer_ix = transfer(
                    TransferParams(
                        from_pubkey=from_keypair.pubkey(),
                        to_pubkey=Pubkey.from_string(to_address),
                        lamports=lamports
                    )
                )
                logger.debug(f"     ✅ Transfer instruction created")
                
                # Get recent blockhash
                logger.debug(f"     🔧 Fetching recent blockhash...")
                blockhash_resp = solana_client.get_latest_blockhash()
                if not blockhash_resp or not blockhash_resp.value:
                    logger.error("     ❌ Failed to get recent blockhash (no response)")
                    return None
                
                recent_blockhash = blockhash_resp.value.blockhash
                logger.debug(f"     ✅ Blockhash: {str(recent_blockhash)[:16]}...")
                
                # Create transaction
                logger.debug(f"     🔧 Creating transaction message...")
                message = Message.new_with_blockhash(
                    [transfer_ix],
                    from_keypair.pubkey(),
                    recent_blockhash
                )
                logger.debug(f"     ✅ Message created")
                
                logger.debug(f"     🔧 Creating signed transaction...")
                transaction = Transaction([from_keypair], message, recent_blockhash)
                logger.debug(f"     ✅ Transaction signed")
                
                # Send transaction (transaction already signed, don't pass keypair again)
                logger.debug(f"     🔧 Sending transaction to RPC...")
                tx_opts = TxOpts(skip_preflight=False, preflight_commitment=Confirmed)
                response = solana_client.send_transaction(
                    transaction,
                    opts=tx_opts
                )
                logger.debug(f"     ✅ RPC response received")
                
                if response and response.value:
                    sig = str(response.value)
                    logger.info(f"     ✅ Transaction sent! Signature: {sig[:16]}...")
                    return sig
                else:
                    logger.error(f"     ❌ send_transaction returned no signature (response={response})")
                    return None
            except Exception as inner_e:
                logger.error(f"     ❌ Error in send_tx inner function: {inner_e}", exc_info=True)
                return None
        
        logger.debug(f"     🔧 Executing send_tx in thread...")
        signature = await asyncio.to_thread(send_tx)
        
        if not signature:
            logger.error(f"     ❌ send_tx returned None")
            return None
        
        logger.debug(f"     ✅ Transaction sent, waiting for confirmation...")
        # Wait for confirmation
        await asyncio.sleep(2)
        confirmed = await verify_sol_transaction(signature)
        if confirmed:
            logger.info(f"     ✅ Transaction CONFIRMED: {signature[:16]}...")
            return signature
        else:
            logger.error(f"     ❌ Transaction NOT confirmed: {signature[:16]}...")
            return None
        
    except Exception as e:
        logger.error(f"     ❌ Error sending SOL transaction: {e}", exc_info=True)
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
        # Set busy timeout on main connection to prevent blocking
        conn.execute("PRAGMA busy_timeout = 5000")  # 5 second timeout
        c = conn.cursor()
        
        # First, recover any stuck 'processing' payments (stuck for >2 minutes)
        # This handles cases where the process crashed during payment processing
        # Reduced from 5 to 2 minutes for faster recovery from lock issues
        logger.debug("🔄 Checking for stuck 'processing' payments...")
        two_min_ago = (datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()
        
        # First, get details of stuck payments before updating
        c.execute("""
            SELECT payment_id, user_id, created_at, expected_wallet
            FROM pending_sol_payments 
            WHERE status = 'processing' 
            AND created_at < ?
        """, (two_min_ago,))
        stuck_payments = c.fetchall()
        
        if stuck_payments:
            logger.warning(f"♻️ [RECOVERY] Found {len(stuck_payments)} stuck 'processing' payment(s)")
            for stuck in stuck_payments:
                logger.warning(f"     Payment {stuck['payment_id']}: user={stuck['user_id']}, wallet={stuck['expected_wallet']}, stuck since {stuck['created_at']}")
        
        # Now update them back to pending
        c.execute("""
            UPDATE pending_sol_payments 
            SET status = 'pending'
            WHERE status = 'processing' 
            AND created_at < ?
        """, (two_min_ago,))
        
        recovered = c.rowcount
        if recovered > 0:
            logger.warning(f"  ✅ [RECOVERY] Recovered {recovered} payment(s) back to 'pending' status")
            conn.commit()
        else:
            logger.debug("  ✅ No stuck payments found")
        
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
            conn.close()
            return
        
        # Convert to list of dicts BEFORE closing connection
        # sqlite3.Row objects need connection to be open
        pending_list = [dict(p) for p in pending]
        logger.debug(f"  📊 Fetched {len(pending_list)} pending payment(s), converted to dicts")
        
        # ✅ CRITICAL: Close main connection AFTER converting to dicts
        # This releases the shared lock and prevents self-deadlock
        conn.close()
        conn = None
        logger.debug(f"  🔒 Main connection closed - shared lock released")
        
        logger.info(f"🔍 Checking {len(pending_list)} pending SOL payment(s)...")
        
        for payment in pending_list:
            payment_id = payment['payment_id']
            user_id = payment['user_id']
            expected_amount = Decimal(str(payment['expected_sol_amount']))
            expected_wallet = payment['expected_wallet']
            created_at = datetime.fromisoformat(payment['created_at'])
            expires_at = datetime.fromisoformat(payment['expires_at'])
            
            # Check if payment expired
            if datetime.now(timezone.utc) > expires_at:
                logger.info(f"⏱️ Payment {payment_id} expired")
                
                # Open a new connection for this update
                expire_conn = None
                try:
                    expire_conn = get_db_connection()
                    expire_c = expire_conn.cursor()
                    expire_c.execute("""
                        UPDATE pending_sol_payments 
                        SET status = 'expired' 
                        WHERE payment_id = ? AND status IN ('pending', 'processing', 'failed')
                    """, (payment_id,))
                    expire_conn.commit()
                except Exception as expire_error:
                    logger.error(f"Error marking payment {payment_id} as expired: {expire_error}")
                finally:
                    if expire_conn:
                        expire_conn.close()
                
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
            
            logger.info(f"💳 [MONITOR] Payment {payment_id}: Expecting {expected_amount:.6f} SOL → {wallet_address[:8]}... (wallet={expected_wallet})")
            logger.debug(f"  Created: {created_at.isoformat()}, Expires: {expires_at.isoformat()}")
            
            # Check if this payment is already being processed by another thread
            # Need new connection since main one was closed
            status_conn = None
            try:
                status_conn = get_db_connection()
                status_c = status_conn.cursor()
                status_c.execute("""
                    SELECT status FROM pending_sol_payments 
                    WHERE payment_id = ?
                """, (payment_id,))
                current_status_row = status_c.fetchone()
                if current_status_row:
                    current_status = current_status_row[0]
                    if current_status == 'processing':
                        logger.debug(f"Payment {payment_id} is already being processed, skipping")
                        continue
                    elif current_status == 'confirmed':
                        logger.debug(f"Payment {payment_id} already confirmed, skipping")
                        continue
            except Exception as status_error:
                logger.error(f"Error checking payment status: {status_error}")
                continue
            finally:
                if status_conn:
                    status_conn.close()
            
            # Check recent transactions to this wallet
            logger.debug(f"  📡 Fetching transactions for {wallet_address[:8]}...")
            transactions = await check_wallet_transactions(wallet_address, limit=20)
            logger.info(f"  📊 Found {len(transactions)} transaction(s) for wallet {wallet_address[:8]}...")
            
            if not transactions:
                logger.debug(f"  ⏭️ No transactions found, skipping payment {payment_id}")
                continue
            
            # Look for matching transaction - STRICT tolerance (0.1% for random offset variance)
            # Random offset adds 0.000001-0.000099 SOL, so 0.1% tolerance is safe
            tolerance = expected_amount * Decimal('0.001')  # 0.1% tolerance (was 1%)
            min_amount = expected_amount - tolerance
            max_amount = expected_amount + tolerance
            logger.info(f"  🔍 [MATCHING] Tolerance range: {min_amount:.6f} to {max_amount:.6f} SOL (±0.1%)")
            logger.debug(f"    Expected: {expected_amount:.6f} SOL ± {tolerance:.6f} SOL")
            
            # Only consider recent transactions (within 30 minutes of payment creation)
            recent_cutoff = created_at - timedelta(minutes=30)
            logger.debug(f"    Recent cutoff: {recent_cutoff.isoformat()} (30 min before payment creation)")
            
            matched_tx = None
            for tx_idx, tx in enumerate(transactions, 1):
                tx_amount = tx['amount_sol']
                tx_signature = tx['signature']
                tx_timestamp = tx.get('timestamp')
                
                logger.debug(f"    TX {tx_idx}/{len(transactions)}: {tx_signature[:16]}... = {tx_amount:.6f} SOL")
                
                # Skip transactions that are too old (before payment was created minus 30 min buffer)
                if tx_timestamp:
                    tx_datetime = datetime.fromtimestamp(tx_timestamp, tz=timezone.utc)
                    if tx_datetime < recent_cutoff:
                        logger.debug(f"      ⏭️ Too old ({tx_datetime.isoformat()}) - skipping")
                        continue
                    logger.debug(f"      ✅ Timestamp OK ({tx_datetime.isoformat()})")
                else:
                    logger.debug(f"      ⚠️ No timestamp, allowing")
                
                # Check if transaction matches expected amount (within tolerance, both upper AND lower bounds)
                if min_amount <= tx_amount <= max_amount:
                    diff = tx_amount - expected_amount
                    diff_percent = (diff / expected_amount * 100) if expected_amount > 0 else 0
                    logger.info(f"  💰 [MATCH FOUND] TX {tx_signature[:16]}... = {tx_amount:.6f} SOL")
                    logger.info(f"      Expected: {expected_amount:.6f} SOL, Diff: {diff:+.6f} SOL ({diff_percent:+.3f}%)")
                    
                    # Check if we already processed this transaction
                    # Need new connection since main one was closed
                    logger.debug(f"      🔍 Checking if TX already processed...")
                    check_conn = None
                    try:
                        check_conn = get_db_connection()
                        check_c = check_conn.cursor()
                        
                        check_c.execute("""
                            SELECT signature FROM processed_sol_transactions 
                            WHERE signature = ?
                        """, (tx_signature,))
                        
                        if check_c.fetchone():
                            logger.warning(f"      ⏭️ TX {tx_signature[:16]}... already processed, skipping")
                            continue
                        logger.debug(f"      ✅ TX not in processed_sol_transactions")
                        
                        # Also verify this transaction isn't already assigned to another payment
                        logger.debug(f"      🔍 Checking if TX assigned to different payment...")
                        check_c.execute("""
                            SELECT payment_id FROM processed_sol_transactions 
                            WHERE signature = ? AND payment_id != ?
                        """, (tx_signature, payment_id))
                        
                        other_payment = check_c.fetchone()
                        if other_payment:
                            logger.warning(f"      ⏭️ TX {tx_signature[:16]}... already used for payment {other_payment[0]}, skipping")
                            continue
                        logger.debug(f"      ✅ TX not assigned to other payment")
                    except Exception as check_error:
                        logger.error(f"      ❌ Error checking transaction status: {check_error}")
                        continue
                    finally:
                        if check_conn:
                            check_conn.close()
                    
                    # Transaction is already confirmed (we only get confirmed txs from check_wallet_transactions)
                    # The 'confirmed' field in tx dict indicates it passed all checks
                    logger.debug(f"      🔍 Checking TX confirmation status...")
                    if not tx.get('confirmed'):
                        logger.warning(f"      ❌ TX {tx_signature[:16]}... not confirmed, skipping")
                        continue
                    logger.debug(f"      ✅ TX confirmed")
                    
                    logger.info(f"  ✅ [PAYMENT MATCHED] Payment {payment_id} ← TX {tx_signature[:16]}...")
                    
                    # CRITICAL: Mark payment as 'processing' FIRST to prevent duplicate processing
                    # Use a separate connection with timeout and retry logic
                    logger.info(f"  🔐 [LOCK] Attempting to acquire payment lock...")
                    payment_locked = False
                    lock_start_time = time.time()
                    lock_conn = None
                    
                    for attempt in range(5):  # Try 5 times (increased from 3)
                        try:
                            logger.debug(f"     Attempt {attempt + 1}/5: Opening lock connection...")
                            lock_conn = get_db_connection()
                            # Set timeout BEFORE any operations
                            lock_conn.execute("PRAGMA busy_timeout = 10000")  # 10 second timeout (increased)
                            lock_c = lock_conn.cursor()
                            
                            logger.debug(f"     Attempt {attempt + 1}/5: Starting transaction with BEGIN IMMEDIATE...")
                            lock_c.execute("BEGIN IMMEDIATE")
                            
                            try:
                                # Check if transaction already processed (double-check race condition protection)
                                logger.debug(f"     Attempt {attempt + 1}/5: Double-checking TX not already processed...")
                                lock_c.execute("""
                                    SELECT signature FROM processed_sol_transactions 
                                    WHERE signature = ?
                                """, (tx_signature,))
                                
                                if lock_c.fetchone():
                                    logger.warning(f"     ⚠️ [LOCK] TX {tx_signature[:16]}... was processed by another thread during lock acquisition")
                                    lock_conn.rollback()
                                    break  # Exit retry loop, move to next TX
                                logger.debug(f"     Attempt {attempt + 1}/5: ✅ TX still unprocessed")
                                
                                # Mark payment as 'processing' immediately (atomic status change)
                                logger.debug(f"     Attempt {attempt + 1}/5: Updating payment status to 'processing'...")
                                lock_c.execute("""
                                    UPDATE pending_sol_payments 
                                    SET status = 'processing'
                                    WHERE payment_id = ? AND status = 'pending'
                                """, (payment_id,))
                                
                                if lock_c.rowcount == 0:
                                    logger.warning(f"     ⚠️ [LOCK] Payment {payment_id} status already changed (another thread acquired lock first)")
                                    lock_conn.rollback()
                                    break  # Exit retry loop, move to next TX
                                logger.debug(f"     Attempt {attempt + 1}/5: ✅ Status updated to 'processing' (rowcount={lock_c.rowcount})")
                                
                                # Commit the lock
                                logger.debug(f"     Attempt {attempt + 1}/5: Committing lock transaction...")
                                lock_conn.commit()
                                lock_duration = time.time() - lock_start_time
                                logger.info(f"  ✅ [LOCK] Payment {payment_id} LOCKED for processing (attempt {attempt + 1}, duration: {lock_duration:.3f}s)")
                                payment_locked = True
                                # Close connection on success
                                lock_conn.close()
                                lock_conn = None
                                break  # Success, exit retry loop
                                
                            except Exception as inner_error:
                                # Rollback on any error within transaction
                                logger.error(f"     ❌ [LOCK] Error within transaction on attempt {attempt + 1}: {inner_error}")
                                lock_conn.rollback()
                                raise  # Re-raise to outer exception handler
                            
                        except sqlite3.OperationalError as lock_error:
                            error_msg = str(lock_error).lower()
                            logger.warning(f"     ⚠️ [LOCK] OperationalError on attempt {attempt + 1}: {lock_error}")
                            
                            # Always rollback and close connection on error
                            if lock_conn:
                                try:
                                    lock_conn.rollback()
                                except:
                                    pass
                                try:
                                    lock_conn.close()
                                except:
                                    pass
                                lock_conn = None
                            
                            if "locked" in error_msg and attempt < 4:
                                # Database locked, retry after exponential backoff
                                retry_delay = 1.0 * (2 ** attempt)  # Exponential: 1s, 2s, 4s, 8s
                                logger.warning(f"     ⏳ [LOCK] Database locked, retrying in {retry_delay:.1f}s...")
                                await asyncio.sleep(retry_delay)
                                continue
                            else:
                                logger.error(f"     ❌ [LOCK] Fatal error on attempt {attempt + 1}: {lock_error}")
                                if attempt == 4:
                                    logger.error(f"     ❌ [LOCK] All 5 attempts exhausted")
                                break
                                
                        except Exception as lock_error:
                            logger.error(f"     ❌ [LOCK] Unexpected error on attempt {attempt + 1}: {lock_error}", exc_info=True)
                            # Always rollback and close connection on error
                            if lock_conn:
                                try:
                                    lock_conn.rollback()
                                except:
                                    pass
                                try:
                                    lock_conn.close()
                                except:
                                    pass
                                lock_conn = None
                            break
                        finally:
                            # Ensure connection is closed
                            if lock_conn:
                                try:
                                    # Only close if transaction is not active
                                    lock_conn.close()
                                except:
                                    pass
                                lock_conn = None
                    
                    # If we couldn't lock the payment, skip to next transaction
                    if not payment_locked:
                        total_lock_duration = time.time() - lock_start_time
                        logger.error(f"  ❌ [LOCK] Failed to acquire lock for payment {payment_id} after 5 attempts ({total_lock_duration:.3f}s total)")
                        logger.error(f"     Payment will be retried in next monitoring cycle (60s)")
                        # Ensure connection is closed even if we failed
                        if lock_conn:
                            try:
                                lock_conn.close()
                            except:
                                pass
                        continue
                    
                    # If payment went to middleman, forward it (outside of any transaction)
                    forward_success = True
                    if expected_wallet == 'middleman':
                        logger.info(f"🔄 Payment to middleman, initiating split forward...")
                        
                        forward_results = await forward_split_payment(
                            payment_id,
                            tx_signature,
                            tx_amount
                        )
                        
                        forward_success = all(forward_results.values())
                        
                        if not forward_success:
                            logger.error(f"❌ Split forward failed: {forward_results}")
                            # Mark payment as 'failed'
                            try:
                                fail_conn = get_db_connection()
                                fail_c = fail_conn.cursor()
                                fail_c.execute("""
                                    UPDATE pending_sol_payments 
                                    SET status = 'failed'
                                    WHERE payment_id = ?
                                """, (payment_id,))
                                fail_conn.commit()
                                fail_conn.close()
                                logger.warning(f"⚠️ Payment {payment_id} marked as failed - manual intervention needed")
                            except Exception as mark_error:
                                logger.error(f"Error marking payment as failed: {mark_error}")
                            continue
                    
                    # Now start NEW atomic transaction for final confirmation
                    logger.info(f"  💾 [CONFIRM] Starting final confirmation transaction...")
                    confirm_conn = None
                    try:
                        # Open NEW connection for confirmation
                        confirm_conn = get_db_connection()
                        confirm_conn.execute("PRAGMA busy_timeout = 10000")
                        confirm_c = confirm_conn.cursor()
                        
                        logger.debug(f"     BEGIN IMMEDIATE on confirmation connection")
                        confirm_c.execute("BEGIN IMMEDIATE")
                        
                        # Final check if transaction was processed during forward
                        logger.debug(f"     Triple-checking TX not processed...")
                        confirm_c.execute("""
                            SELECT signature FROM processed_sol_transactions 
                            WHERE signature = ?
                        """, (tx_signature,))
                        
                        if confirm_c.fetchone():
                            logger.warning(f"  ⚠️ [CONFIRM] TX {tx_signature[:16]}... was processed during forwarding, rolling back")
                            confirm_conn.rollback()
                            continue
                        logger.debug(f"     ✅ TX still unprocessed")
                        
                        # Mark transaction as processed (atomic with payment confirmation)
                        logger.debug(f"     Inserting into processed_sol_transactions...")
                        confirm_c.execute("""
                            INSERT INTO processed_sol_transactions 
                            (signature, payment_id, processed_at, amount)
                            VALUES (?, ?, ?, ?)
                        """, (
                            tx_signature,
                            payment_id,
                            datetime.now(timezone.utc).isoformat(),
                            float(tx_amount)
                        ))
                        logger.debug(f"     ✅ TX marked as processed")
                        
                        # Mark payment as confirmed
                        logger.debug(f"     Updating payment status to 'confirmed'...")
                        confirm_c.execute("""
                            UPDATE pending_sol_payments 
                            SET status = 'confirmed', transaction_signature = ?
                            WHERE payment_id = ?
                        """, (tx_signature, payment_id))
                        logger.debug(f"     ✅ Payment marked as confirmed")
                        
                        # Commit atomic transaction
                        logger.debug(f"     Committing final confirmation...")
                        confirm_conn.commit()
                        logger.info(f"  ✅ [CONFIRM] Payment {payment_id} and TX {tx_signature[:16]}... ATOMICALLY confirmed")
                        
                    except Exception as atomic_error:
                        logger.error(f"Error in atomic transaction processing: {atomic_error}")
                        if confirm_conn:
                            try:
                                confirm_conn.rollback()
                            except:
                                pass
                        continue
                    finally:
                        if confirm_conn:
                            try:
                                confirm_conn.close()
                            except:
                                pass
                    
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
                else:
                    # Transaction amount doesn't match
                    if tx_amount < min_amount:
                        shortage = min_amount - tx_amount
                        logger.debug(f"      ⏭️ Amount too low by {shortage:.6f} SOL ({min_amount:.6f} needed)")
                    else:
                        excess = tx_amount - max_amount
                        logger.debug(f"      ⏭️ Amount too high by {excess:.6f} SOL ({max_amount:.6f} max)")
        
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

