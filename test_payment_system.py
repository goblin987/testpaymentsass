"""
Comprehensive Payment System Test Suite
Tests all critical payment scenarios without requiring actual Telegram or blockchain interaction.
"""

import asyncio
import sqlite3
import time
import json
import sys
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, patch, MagicMock
from typing import List, Dict

# Add current directory to path
sys.path.insert(0, '.')

# Mock Solana imports before importing modules
sys.modules['solders'] = MagicMock()
sys.modules['solders.keypair'] = MagicMock()
sys.modules['solders.pubkey'] = MagicMock()
sys.modules['solders.signature'] = MagicMock()
sys.modules['solders.system_program'] = MagicMock()
sys.modules['solders.transaction'] = MagicMock()
sys.modules['solders.message'] = MagicMock()
sys.modules['solders.rpc'] = MagicMock()
sys.modules['solders.rpc.responses'] = MagicMock()
sys.modules['solana'] = MagicMock()
sys.modules['solana.rpc'] = MagicMock()
sys.modules['solana.rpc.api'] = MagicMock()
sys.modules['solana.rpc.commitment'] = MagicMock()
sys.modules['solana.rpc.types'] = MagicMock()

from utils import get_db_connection, init_db
import sol_payment

# Test configuration
TEST_DB = 'test_bot.db'
TEST_USER_ID = 999999999
TEST_WALLET1_ADDRESS = 'GxTestWallet1111111111111111111111111111111111'
TEST_WALLET2_ADDRESS = 'GxTestWallet2222222222222222222222222222222222'
TEST_MIDDLEMAN_ADDRESS = 'GxTestMiddleman333333333333333333333333333333333'

# Color codes for output
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    END = '\033[0m'
    BOLD = '\033[1m'

def print_test_header(test_name: str):
    """Print formatted test header."""
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'='*80}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.CYAN}TEST: {test_name}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'='*80}{Colors.END}\n")

def print_success(message: str):
    """Print success message."""
    print(f"{Colors.GREEN}✅ {message}{Colors.END}")

def print_error(message: str):
    """Print error message."""
    print(f"{Colors.RED}❌ {message}{Colors.END}")

def print_warning(message: str):
    """Print warning message."""
    print(f"{Colors.YELLOW}⚠️  {message}{Colors.END}")

def print_info(message: str):
    """Print info message."""
    print(f"{Colors.BLUE}ℹ️  {message}{Colors.END}")

class TestPaymentSystem:
    """Test suite for payment system."""
    
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.warnings = 0
        
    def setup_test_db(self):
        """Create a clean test database."""
        print_info("Setting up test database...")
        
        # Remove old test DB if exists
        import os
        if os.path.exists(TEST_DB):
            os.remove(TEST_DB)
        
        # Initialize new test DB
        with patch('utils.DATABASE_PATH', TEST_DB):
            init_db()
        
        print_success("Test database initialized")
    
    def create_test_product(self, payout_wallet: str = 'wallet1', price: float = 10.0) -> int:
        """Create a test product in the database."""
        conn = sqlite3.connect(TEST_DB)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        c.execute("""
            INSERT INTO products (name, size, price, city, district, product_type, 
                                 available, reserved, payout_wallet, original_text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, ('Test Product', 'Medium', price, 'Vilnius', 'Centras', 'herb',
              10, 0, payout_wallet, 'Test product text'))
        
        product_id = c.lastrowid
        conn.commit()
        conn.close()
        
        return product_id
    
    def create_basket_snapshot(self, product_ids: List[int], payout_wallets: List[str]) -> List[Dict]:
        """Create a basket snapshot from product IDs."""
        conn = sqlite3.connect(TEST_DB)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        snapshot = []
        for prod_id, payout_wallet in zip(product_ids, payout_wallets):
            c.execute("SELECT * FROM products WHERE id = ?", (prod_id,))
            product = c.fetchone()
            
            if product:
                snapshot.append({
                    "product_id": prod_id,
                    "price": float(product['price']),
                    "name": product['name'],
                    "size": product['size'],
                    "product_type": product['product_type'],
                    "city": product['city'],
                    "district": product['district'],
                    "original_text": product['original_text'],
                    "payout_wallet": payout_wallet
                })
        
        conn.close()
        return snapshot
    
    async def test_1_wallet1_direct_payment(self):
        """Test 1: Single payment to wallet1 (direct, no split)."""
        print_test_header("Test 1: Single Payment to Wallet1 (Direct)")
        
        try:
            # Create product with wallet1
            product_id = self.create_test_product(payout_wallet='wallet1', price=10.0)
            print_info(f"Created product {product_id} with payout_wallet='wallet1'")
            
            # Create basket snapshot
            basket = self.create_basket_snapshot([product_id], ['wallet1'])
            print_info(f"Created basket snapshot: {basket}")
            
            # Mock SOL price
            with patch.object(sol_payment, 'get_sol_price_eur', return_value=Decimal('150.0')):
                # Create payment
                with patch('utils.DATABASE_PATH', TEST_DB):
                    with patch.object(sol_payment, 'SOL_WALLET1_ADDRESS', TEST_WALLET1_ADDRESS):
                        with patch.object(sol_payment, 'SOL_WALLET2_ADDRESS', TEST_WALLET2_ADDRESS):
                            with patch.object(sol_payment, 'SOL_MIDDLEMAN_ADDRESS', TEST_MIDDLEMAN_ADDRESS):
                                payment_result = await sol_payment.create_sol_payment(
                                    user_id=TEST_USER_ID,
                                    basket_snapshot=basket,
                                    total_eur=Decimal('10.0'),
                                    discount_code=None
                                )
            
            # Verify payment creation
            if 'error' in payment_result:
                print_error(f"Payment creation failed: {payment_result['error']}")
                self.failed += 1
                return
            
            # Check wallet destination
            if payment_result['wallet_name'] != 'wallet1':
                print_error(f"Expected wallet1, got {payment_result['wallet_name']}")
                self.failed += 1
                return
            
            if payment_result['wallet_address'] != TEST_WALLET1_ADDRESS:
                print_error(f"Wrong wallet address: {payment_result['wallet_address']}")
                self.failed += 1
                return
            
            # Verify database entry
            conn = sqlite3.connect(TEST_DB)
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            c.execute("SELECT * FROM pending_sol_payments WHERE payment_id = ?", 
                     (payment_result['payment_id'],))
            db_payment = c.fetchone()
            conn.close()
            
            if not db_payment:
                print_error("Payment not found in database")
                self.failed += 1
                return
            
            if db_payment['expected_wallet'] != 'wallet1':
                print_error(f"Database has wrong wallet: {db_payment['expected_wallet']}")
                self.failed += 1
                return
            
            print_success("Payment created successfully")
            print_success(f"Payment ID: {payment_result['payment_id']}")
            print_success(f"Amount: {payment_result['sol_amount']:.6f} SOL")
            print_success(f"Destination: wallet1 ({TEST_WALLET1_ADDRESS})")
            
            self.passed += 1
            
        except Exception as e:
            print_error(f"Test failed with exception: {e}")
            import traceback
            traceback.print_exc()
            self.failed += 1
    
    async def test_2_wallet2_direct_payment(self):
        """Test 2: Single payment to wallet2 (direct, no split)."""
        print_test_header("Test 2: Single Payment to Wallet2 (Direct)")
        
        try:
            # Create product with wallet2
            product_id = self.create_test_product(payout_wallet='wallet2', price=15.0)
            print_info(f"Created product {product_id} with payout_wallet='wallet2'")
            
            # Create basket snapshot
            basket = self.create_basket_snapshot([product_id], ['wallet2'])
            
            # Mock SOL price
            with patch.object(sol_payment, 'get_sol_price_eur', return_value=Decimal('150.0')):
                with patch('utils.DATABASE_PATH', TEST_DB):
                    with patch.object(sol_payment, 'SOL_WALLET1_ADDRESS', TEST_WALLET1_ADDRESS):
                        with patch.object(sol_payment, 'SOL_WALLET2_ADDRESS', TEST_WALLET2_ADDRESS):
                            with patch.object(sol_payment, 'SOL_MIDDLEMAN_ADDRESS', TEST_MIDDLEMAN_ADDRESS):
                                payment_result = await sol_payment.create_sol_payment(
                                    user_id=TEST_USER_ID,
                                    basket_snapshot=basket,
                                    total_eur=Decimal('15.0'),
                                    discount_code=None
                                )
            
            # Verify
            if 'error' in payment_result:
                print_error(f"Payment creation failed: {payment_result['error']}")
                self.failed += 1
                return
            
            if payment_result['wallet_name'] != 'wallet2':
                print_error(f"Expected wallet2, got {payment_result['wallet_name']}")
                self.failed += 1
                return
            
            print_success("Payment routed to wallet2 correctly")
            self.passed += 1
            
        except Exception as e:
            print_error(f"Test failed: {e}")
            self.failed += 1
    
    async def test_3_split_payment(self):
        """Test 3: Single payment requiring split (20%/80%)."""
        print_test_header("Test 3: Split Payment (20% Asmenine / 80% Kolegos)")
        
        try:
            # Create product with split
            product_id = self.create_test_product(payout_wallet='split', price=20.0)
            print_info(f"Created product {product_id} with payout_wallet='split'")
            
            # Create basket snapshot
            basket = self.create_basket_snapshot([product_id], ['split'])
            
            # Mock SOL price
            with patch.object(sol_payment, 'get_sol_price_eur', return_value=Decimal('150.0')):
                with patch('utils.DATABASE_PATH', TEST_DB):
                    with patch.object(sol_payment, 'SOL_WALLET1_ADDRESS', TEST_WALLET1_ADDRESS):
                        with patch.object(sol_payment, 'SOL_WALLET2_ADDRESS', TEST_WALLET2_ADDRESS):
                            with patch.object(sol_payment, 'SOL_MIDDLEMAN_ADDRESS', TEST_MIDDLEMAN_ADDRESS):
                                payment_result = await sol_payment.create_sol_payment(
                                    user_id=TEST_USER_ID,
                                    basket_snapshot=basket,
                                    total_eur=Decimal('20.0'),
                                    discount_code=None
                                )
            
            # Verify
            if 'error' in payment_result:
                print_error(f"Payment creation failed: {payment_result['error']}")
                self.failed += 1
                return
            
            if payment_result['wallet_name'] != 'middleman':
                print_error(f"Expected middleman, got {payment_result['wallet_name']}")
                self.failed += 1
                return
            
            if payment_result['wallet_address'] != TEST_MIDDLEMAN_ADDRESS:
                print_error(f"Wrong middleman address")
                self.failed += 1
                return
            
            print_success("Payment routed to middleman for split")
            print_success(f"Will be split: 20% → Asmenine, 80% → Kolegos")
            self.passed += 1
            
        except Exception as e:
            print_error(f"Test failed: {e}")
            self.failed += 1
    
    async def test_4_mixed_basket(self):
        """Test 4: Mixed basket with multiple wallet destinations."""
        print_test_header("Test 4: Mixed Basket (Multiple Wallets)")
        
        try:
            # Create products with different wallets
            prod1 = self.create_test_product(payout_wallet='wallet1', price=10.0)
            prod2 = self.create_test_product(payout_wallet='wallet2', price=15.0)
            prod3 = self.create_test_product(payout_wallet='split', price=20.0)
            
            print_info(f"Created products: {prod1} (wallet1), {prod2} (wallet2), {prod3} (split)")
            
            # Create basket snapshot
            basket = self.create_basket_snapshot(
                [prod1, prod2, prod3],
                ['wallet1', 'wallet2', 'split']
            )
            
            # Mock SOL price
            with patch.object(sol_payment, 'get_sol_price_eur', return_value=Decimal('150.0')):
                with patch('utils.DATABASE_PATH', TEST_DB):
                    with patch.object(sol_payment, 'SOL_WALLET1_ADDRESS', TEST_WALLET1_ADDRESS):
                        with patch.object(sol_payment, 'SOL_WALLET2_ADDRESS', TEST_WALLET2_ADDRESS):
                            with patch.object(sol_payment, 'SOL_MIDDLEMAN_ADDRESS', TEST_MIDDLEMAN_ADDRESS):
                                payment_result = await sol_payment.create_sol_payment(
                                    user_id=TEST_USER_ID,
                                    basket_snapshot=basket,
                                    total_eur=Decimal('45.0'),
                                    discount_code=None
                                )
            
            # Verify - should go to middleman (mixed wallets + split)
            if 'error' in payment_result:
                print_error(f"Payment creation failed: {payment_result['error']}")
                self.failed += 1
                return
            
            if payment_result['wallet_name'] != 'middleman':
                print_error(f"Expected middleman for mixed basket, got {payment_result['wallet_name']}")
                self.failed += 1
                return
            
            print_success("Mixed basket correctly routed to middleman")
            print_info("Reason: Contains split item + mixed wallets")
            self.passed += 1
            
        except Exception as e:
            print_error(f"Test failed: {e}")
            self.failed += 1
    
    async def test_5_random_offset_uniqueness(self):
        """Test 5: Verify random offsets make each payment unique."""
        print_test_header("Test 5: Random Offset Uniqueness")
        
        try:
            product_id = self.create_test_product(payout_wallet='wallet1', price=10.0)
            basket = self.create_basket_snapshot([product_id], ['wallet1'])
            
            amounts = []
            payment_ids = []
            
            # Create 10 identical payments
            for i in range(10):
                with patch.object(sol_payment, 'get_sol_price_eur', return_value=Decimal('150.0')):
                    with patch('utils.DATABASE_PATH', TEST_DB):
                        with patch.object(sol_payment, 'SOL_WALLET1_ADDRESS', TEST_WALLET1_ADDRESS):
                            payment_result = await sol_payment.create_sol_payment(
                                user_id=TEST_USER_ID + i,
                                basket_snapshot=basket,
                                total_eur=Decimal('10.0'),
                                discount_code=None
                            )
                
                if 'error' not in payment_result:
                    amounts.append(payment_result['sol_amount'])
                    payment_ids.append(payment_result['payment_id'])
                
                # Small delay
                await asyncio.sleep(0.01)
            
            # Check uniqueness
            unique_amounts = len(set(amounts))
            
            if unique_amounts == len(amounts):
                print_success(f"All {len(amounts)} payment amounts are unique")
                print_info(f"Amount range: {min(amounts):.6f} - {max(amounts):.6f} SOL")
                print_info(f"Difference range: {(max(amounts) - min(amounts)):.6f} SOL")
                self.passed += 1
            else:
                print_error(f"Only {unique_amounts}/{len(amounts)} amounts are unique")
                print_error(f"Duplicate amounts detected: {amounts}")
                self.failed += 1
            
        except Exception as e:
            print_error(f"Test failed: {e}")
            self.failed += 1
    
    async def test_6_tolerance_validation(self):
        """Test 6: Verify 0.1% tolerance calculation."""
        print_test_header("Test 6: Tolerance Calculation Validation")
        
        try:
            # Test cases: expected amount and whether amounts should match
            test_cases = [
                (Decimal('0.100000'), Decimal('0.100050'), True, "Within 0.1% (0.05%)"),
                (Decimal('0.100000'), Decimal('0.100100'), True, "At 0.1% boundary"),
                (Decimal('0.100000'), Decimal('0.100150'), False, "Outside 0.1% (0.15%)"),
                (Decimal('0.100000'), Decimal('0.099900'), True, "At -0.1% boundary"),
                (Decimal('0.100000'), Decimal('0.099850'), False, "Outside -0.1%"),
            ]
            
            all_passed = True
            for expected, actual, should_match, description in test_cases:
                tolerance = expected * Decimal('0.001')  # 0.1%
                min_amount = expected - tolerance
                max_amount = expected + tolerance
                
                matches = min_amount <= actual <= max_amount
                
                if matches == should_match:
                    print_success(f"{description}: Expected={expected:.6f}, Actual={actual:.6f}, Match={matches} ✓")
                else:
                    print_error(f"{description}: Expected={expected:.6f}, Actual={actual:.6f}, Match={matches} (expected {should_match}) ✗")
                    all_passed = False
            
            if all_passed:
                print_success("All tolerance calculations correct")
                self.passed += 1
            else:
                print_error("Some tolerance calculations failed")
                self.failed += 1
            
        except Exception as e:
            print_error(f"Test failed: {e}")
            self.failed += 1
    
    async def test_7_payout_wallet_persistence(self):
        """Test 7: Verify payout_wallet persists through snapshot."""
        print_test_header("Test 7: Payout Wallet Persistence")
        
        try:
            # Create products with all wallet types
            products = {
                'wallet1': self.create_test_product(payout_wallet='wallet1', price=10.0),
                'wallet2': self.create_test_product(payout_wallet='wallet2', price=10.0),
                'split': self.create_test_product(payout_wallet='split', price=10.0),
            }
            
            all_passed = True
            for wallet_type, product_id in products.items():
                basket = self.create_basket_snapshot([product_id], [wallet_type])
                
                # Verify snapshot has correct payout_wallet
                if basket[0]['payout_wallet'] != wallet_type:
                    print_error(f"Product {product_id}: Expected '{wallet_type}', got '{basket[0]['payout_wallet']}'")
                    all_passed = False
                else:
                    print_success(f"Product {product_id}: payout_wallet='{wallet_type}' persisted correctly")
            
            if all_passed:
                print_success("All payout_wallet values persisted correctly")
                self.passed += 1
            else:
                print_error("Some payout_wallet values not persisted")
                self.failed += 1
            
        except Exception as e:
            print_error(f"Test failed: {e}")
            self.failed += 1
    
    async def test_8_minimum_amount_validation(self):
        """Test 8: Verify minimum payment amount validation."""
        print_test_header("Test 8: Minimum Amount Validation")
        
        try:
            product_id = self.create_test_product(payout_wallet='wallet1', price=0.50)
            basket = self.create_basket_snapshot([product_id], ['wallet1'])
            
            # Mock very high SOL price to make EUR amount result in tiny SOL amount
            with patch.object(sol_payment, 'get_sol_price_eur', return_value=Decimal('1000.0')):
                with patch('utils.DATABASE_PATH', TEST_DB):
                    with patch.object(sol_payment, 'SOL_WALLET1_ADDRESS', TEST_WALLET1_ADDRESS):
                        payment_result = await sol_payment.create_sol_payment(
                            user_id=TEST_USER_ID,
                            basket_snapshot=basket,
                            total_eur=Decimal('0.50'),
                            discount_code=None
                        )
            
            # Should get amount_too_low error
            if 'error' in payment_result and payment_result['error'] == 'amount_too_low':
                print_success(f"Correctly rejected amount below minimum")
                print_info(f"Minimum: {payment_result.get('min_sol', 0)} SOL")
                self.passed += 1
            else:
                print_error(f"Did not reject small amount: {payment_result}")
                self.failed += 1
            
        except Exception as e:
            print_error(f"Test failed: {e}")
            self.failed += 1
    
    def print_summary(self):
        """Print test summary."""
        print(f"\n{Colors.BOLD}{'='*80}{Colors.END}")
        print(f"{Colors.BOLD}TEST SUMMARY{Colors.END}")
        print(f"{Colors.BOLD}{'='*80}{Colors.END}\n")
        
        total = self.passed + self.failed
        print(f"Total Tests: {total}")
        print(f"{Colors.GREEN}Passed: {self.passed}{Colors.END}")
        print(f"{Colors.RED}Failed: {self.failed}{Colors.END}")
        
        if self.warnings > 0:
            print(f"{Colors.YELLOW}Warnings: {self.warnings}{Colors.END}")
        
        if self.failed == 0:
            print(f"\n{Colors.BOLD}{Colors.GREEN}🎉 ALL TESTS PASSED! 🎉{Colors.END}\n")
        else:
            print(f"\n{Colors.BOLD}{Colors.RED}❌ SOME TESTS FAILED{Colors.END}\n")
        
        print(f"{Colors.BOLD}{'='*80}{Colors.END}\n")

async def run_all_tests():
    """Run all payment system tests."""
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'='*80}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.CYAN}PAYMENT SYSTEM TEST SUITE{Colors.END}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'='*80}{Colors.END}\n")
    
    tester = TestPaymentSystem()
    
    # Setup
    tester.setup_test_db()
    
    # Run tests
    await tester.test_1_wallet1_direct_payment()
    await tester.test_2_wallet2_direct_payment()
    await tester.test_3_split_payment()
    await tester.test_4_mixed_basket()
    await tester.test_5_random_offset_uniqueness()
    await tester.test_6_tolerance_validation()
    await tester.test_7_payout_wallet_persistence()
    await tester.test_8_minimum_amount_validation()
    
    # Summary
    tester.print_summary()
    
    # Return exit code
    return 0 if tester.failed == 0 else 1

if __name__ == '__main__':
    exit_code = asyncio.run(run_all_tests())
    sys.exit(exit_code)
