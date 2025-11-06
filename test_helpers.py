"""
Test Helpers and Mock Data Generators
Provides mock Solana RPC responses and test data generators for payment testing.
"""

import random
import time
from decimal import Decimal
from typing import List, Dict, Optional
from dataclasses import dataclass
from datetime import datetime, timezone

@dataclass
class MockTransaction:
    """Mock Solana transaction."""
    signature: str
    amount_sol: Decimal
    from_address: str
    to_address: str
    timestamp: int
    confirmed: bool = True
    block_time: Optional[int] = None
    slot: Optional[int] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary format expected by payment system."""
        return {
            'signature': self.signature,
            'amount_sol': self.amount_sol,
            'timestamp': self.timestamp or self.block_time,
            'confirmed': self.confirmed
        }

class MockSolanaRPC:
    """Mock Solana RPC client for testing."""
    
    def __init__(self):
        self.wallets = {}  # wallet_address -> balance in lamports
        self.transactions = {}  # wallet_address -> List[MockTransaction]
        self.call_count = 0
        self.rate_limit_after = None  # Simulate rate limiting after N calls
        
    def set_wallet_balance(self, address: str, balance_sol: Decimal):
        """Set wallet balance in SOL."""
        self.wallets[address] = int(balance_sol * Decimal('1000000000'))
    
    def add_transaction(self, tx: MockTransaction):
        """Add a mock transaction to a wallet."""
        if tx.to_address not in self.transactions:
            self.transactions[tx.to_address] = []
        self.transactions[tx.to_address].append(tx)
    
    def get_balance(self, address: str) -> Dict:
        """Mock get_balance RPC call."""
        self.call_count += 1
        
        if self.rate_limit_after and self.call_count > self.rate_limit_after:
            raise Exception("429 Too Many Requests")
        
        balance = self.wallets.get(address, 0)
        
        return {
            'value': balance
        }
    
    def get_signatures_for_address(self, address: str, limit: int = 20) -> Dict:
        """Mock get_signatures_for_address RPC call."""
        self.call_count += 1
        
        if self.rate_limit_after and self.call_count > self.rate_limit_after:
            raise Exception("429 Too Many Requests")
        
        transactions = self.transactions.get(address, [])
        
        # Convert to signature info format
        signatures = []
        for tx in transactions[:limit]:
            signatures.append({
                'signature': tx.signature,
                'block_time': tx.block_time or tx.timestamp,
                'err': None  # No error
            })
        
        return {'value': signatures}
    
    def get_transaction(self, signature: str) -> Dict:
        """Mock get_transaction RPC call."""
        self.call_count += 1
        
        if self.rate_limit_after and self.call_count > self.rate_limit_after:
            raise Exception("429 Too Many Requests")
        
        # Find transaction
        for wallet_txs in self.transactions.values():
            for tx in wallet_txs:
                if tx.signature == signature:
                    # Create mock transaction response
                    return {
                        'value': {
                            'transaction': {
                                'meta': {
                                    'err': None,
                                    'pre_balances': [100000000],  # Mock balance before
                                    'post_balances': [100000000 + int(tx.amount_sol * Decimal('1000000000'))]  # After
                                },
                                'transaction': {
                                    'message': {
                                        'account_keys': [
                                            {'pubkey': tx.from_address},
                                            {'pubkey': tx.to_address}
                                        ]
                                    }
                                }
                            },
                            'block_time': tx.block_time or tx.timestamp,
                            'slot': tx.slot or 123456
                        }
                    }
        
        return {'value': None}

class TestDataGenerator:
    """Generate test data for payment scenarios."""
    
    @staticmethod
    def generate_signature() -> str:
        """Generate a mock transaction signature."""
        chars = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
        return ''.join(random.choice(chars) for _ in range(88))
    
    @staticmethod
    def generate_sol_amount(base_eur: Decimal, sol_price_eur: Decimal, add_offset: bool = True) -> Decimal:
        """Generate SOL amount for EUR value with optional random offset."""
        # Calculate base amount
        sol_amount = (base_eur / sol_price_eur).quantize(Decimal('0.000001'))
        
        # Add 1% buffer
        sol_amount = (sol_amount * Decimal('1.01')).quantize(Decimal('0.000001'))
        
        # Add random offset if requested
        if add_offset:
            offset = Decimal(str(random.randint(1, 99))) / Decimal('1000000')
            sol_amount += offset
        
        return sol_amount
    
    @staticmethod
    def create_mock_transaction(
        from_addr: str,
        to_addr: str,
        amount_sol: Decimal,
        timestamp: Optional[int] = None
    ) -> MockTransaction:
        """Create a mock transaction."""
        return MockTransaction(
            signature=TestDataGenerator.generate_signature(),
            amount_sol=amount_sol,
            from_address=from_addr,
            to_address=to_addr,
            timestamp=timestamp or int(time.time()),
            confirmed=True,
            block_time=timestamp or int(time.time()),
            slot=random.randint(100000, 200000)
        )
    
    @staticmethod
    def create_concurrent_payments(
        count: int,
        base_eur: Decimal,
        sol_price_eur: Decimal,
        to_address: str
    ) -> List[MockTransaction]:
        """Create multiple concurrent payments with unique random offsets."""
        from_addr = "SenderAddress" + TestDataGenerator.generate_signature()[:20]
        
        transactions = []
        timestamp = int(time.time())
        
        for i in range(count):
            amount = TestDataGenerator.generate_sol_amount(base_eur, sol_price_eur, add_offset=True)
            tx = TestDataGenerator.create_mock_transaction(
                from_addr=from_addr + str(i),
                to_addr=to_address,
                amount_sol=amount,
                timestamp=timestamp + i  # Slightly different timestamps
            )
            transactions.append(tx)
        
        return transactions
    
    @staticmethod
    def create_test_basket(products_config: List[Dict]) -> List[Dict]:
        """
        Create a test basket snapshot.
        
        Args:
            products_config: List of dicts with keys: payout_wallet, price, name
        
        Returns:
            List of basket item dictionaries
        """
        basket = []
        
        for i, config in enumerate(products_config):
            basket.append({
                "product_id": 1000 + i,
                "price": float(config.get('price', 10.0)),
                "name": config.get('name', f'Test Product {i+1}'),
                "size": config.get('size', 'Medium'),
                "product_type": config.get('product_type', 'herb'),
                "city": config.get('city', 'Vilnius'),
                "district": config.get('district', 'Centras'),
                "original_text": config.get('original_text', 'Test product'),
                "payout_wallet": config.get('payout_wallet', 'wallet1')
            })
        
        return basket

class PaymentScenario:
    """Pre-configured payment test scenarios."""
    
    @staticmethod
    def scenario_wallet1_direct():
        """Scenario: Single item, wallet1, direct payment."""
        return {
            'name': 'Wallet1 Direct',
            'basket': TestDataGenerator.create_test_basket([
                {'payout_wallet': 'wallet1', 'price': 10.0}
            ]),
            'expected_wallet': 'wallet1',
            'total_eur': Decimal('10.0')
        }
    
    @staticmethod
    def scenario_wallet2_direct():
        """Scenario: Single item, wallet2, direct payment."""
        return {
            'name': 'Wallet2 Direct',
            'basket': TestDataGenerator.create_test_basket([
                {'payout_wallet': 'wallet2', 'price': 15.0}
            ]),
            'expected_wallet': 'wallet2',
            'total_eur': Decimal('15.0')
        }
    
    @staticmethod
    def scenario_split_payment():
        """Scenario: Single item requiring split (20%/80%)."""
        return {
            'name': 'Split Payment',
            'basket': TestDataGenerator.create_test_basket([
                {'payout_wallet': 'split', 'price': 20.0}
            ]),
            'expected_wallet': 'middleman',
            'total_eur': Decimal('20.0'),
            'requires_split': True
        }
    
    @staticmethod
    def scenario_mixed_basket():
        """Scenario: Multiple items with different payout wallets."""
        return {
            'name': 'Mixed Basket',
            'basket': TestDataGenerator.create_test_basket([
                {'payout_wallet': 'wallet1', 'price': 10.0, 'name': 'Product A'},
                {'payout_wallet': 'wallet2', 'price': 15.0, 'name': 'Product B'},
                {'payout_wallet': 'split', 'price': 20.0, 'name': 'Product C'}
            ]),
            'expected_wallet': 'middleman',
            'total_eur': Decimal('45.0'),
            'requires_split': True  # Has split item
        }
    
    @staticmethod
    def scenario_same_wallet_multiple():
        """Scenario: Multiple items, all same wallet (should go direct)."""
        return {
            'name': 'Multiple Items Same Wallet',
            'basket': TestDataGenerator.create_test_basket([
                {'payout_wallet': 'wallet1', 'price': 10.0, 'name': 'Product A'},
                {'payout_wallet': 'wallet1', 'price': 12.0, 'name': 'Product B'},
                {'payout_wallet': 'wallet1', 'price': 8.0, 'name': 'Product C'}
            ]),
            'expected_wallet': 'wallet1',
            'total_eur': Decimal('30.0')
        }
    
    @staticmethod
    def all_scenarios():
        """Get all test scenarios."""
        return [
            PaymentScenario.scenario_wallet1_direct(),
            PaymentScenario.scenario_wallet2_direct(),
            PaymentScenario.scenario_split_payment(),
            PaymentScenario.scenario_mixed_basket(),
            PaymentScenario.scenario_same_wallet_multiple()
        ]

# Example usage
if __name__ == '__main__':
    print("=== Test Data Generator Examples ===\n")
    
    # Example 1: Generate transaction signature
    sig = TestDataGenerator.generate_signature()
    print(f"Mock Signature: {sig}\n")
    
    # Example 2: Calculate SOL amount
    sol_amount = TestDataGenerator.generate_sol_amount(
        base_eur=Decimal('10.0'),
        sol_price_eur=Decimal('150.0'),
        add_offset=True
    )
    print(f"SOL Amount for 10 EUR at 150 EUR/SOL: {sol_amount:.6f} SOL\n")
    
    # Example 3: Create concurrent payments
    txs = TestDataGenerator.create_concurrent_payments(
        count=3,
        base_eur=Decimal('10.0'),
        sol_price_eur=Decimal('150.0'),
        to_address="GxTestWallet111111111111111111111111111111111"
    )
    
    print("Concurrent Payments:")
    for i, tx in enumerate(txs, 1):
        print(f"  TX {i}: {tx.amount_sol:.6f} SOL (sig: {tx.signature[:16]}...)")
    print()
    
    # Example 4: Create test basket
    basket = TestDataGenerator.create_test_basket([
        {'payout_wallet': 'wallet1', 'price': 10.0},
        {'payout_wallet': 'split', 'price': 20.0}
    ])
    
    print("Test Basket:")
    for item in basket:
        print(f"  - {item['name']}: {item['price']} EUR → {item['payout_wallet']}")
    print()
    
    # Example 5: All scenarios
    print("Available Test Scenarios:")
    for scenario in PaymentScenario.all_scenarios():
        print(f"  - {scenario['name']}: {len(scenario['basket'])} item(s), "
              f"{scenario['total_eur']} EUR → {scenario['expected_wallet']}")
