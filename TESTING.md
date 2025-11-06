# Payment System Testing Guide

This document explains how to test the Solana payment system using the provided test scripts.

## Test Files

- **`test_payment_system.py`** - Main test suite with 8+ comprehensive payment scenarios
- **`test_helpers.py`** - Mock data generators and RPC response utilities
- **`TESTING.md`** - This file (testing documentation)

## Prerequisites

```bash
# Install requirements
pip install -r requirements.txt

# Ensure you're in the project root directory
cd /path/to/taxv2bot-main
```

## Running Tests

### Run All Tests

```bash
python test_payment_system.py
```

Expected output:
```
================================================================================
PAYMENT SYSTEM TEST SUITE
================================================================================

ℹ️  Setting up test database...
✅ Test database initialized

================================================================================
TEST: Test 1: Single Payment to Wallet1 (Direct)
================================================================================

ℹ️  Created product 1 with payout_wallet='wallet1'
...
✅ Payment created successfully
✅ Payment ID: SOL_999999999_1234567890_abc123
✅ Amount: 0.067567 SOL
✅ Destination: wallet1 (GxTestWallet1111111111111111111111111111111111)

================================================================================
TEST SUMMARY
================================================================================

Total Tests: 8
Passed: 8
Failed: 0

🎉 ALL TESTS PASSED! 🎉
```

### Run Individual Test Helper Examples

```bash
python test_helpers.py
```

This will demonstrate:
- Mock signature generation
- SOL amount calculation
- Concurrent payment simulation
- Test basket creation
- All available test scenarios

## Test Scenarios Covered

### Test 1: Wallet1 Direct Payment
- **Purpose**: Verify single product payment to wallet1
- **Validates**: 
  - Correct wallet routing (wallet1)
  - Database persistence
  - Amount calculation with random offset

### Test 2: Wallet2 Direct Payment
- **Purpose**: Verify single product payment to wallet2
- **Validates**:
  - Correct wallet routing (wallet2)
  - Different wallet handling

### Test 3: Split Payment
- **Purpose**: Verify payment requiring 20%/80% split
- **Validates**:
  - Middleman routing for split products
  - Split configuration detection

### Test 4: Mixed Basket
- **Purpose**: Multiple products with different payout wallets
- **Validates**:
  - Middleman routing for mixed baskets
  - Correct handling of multiple wallet types

### Test 5: Random Offset Uniqueness
- **Purpose**: Ensure each payment has unique amount
- **Validates**:
  - Random offset generation (0.000001-0.000099 SOL)
  - No duplicate amounts in 10 identical payments
  - Uniqueness prevents payment collisions

### Test 6: Tolerance Validation
- **Purpose**: Verify 0.1% tolerance calculation
- **Validates**:
  - Upper bound matching
  - Lower bound matching
  - Rejection outside tolerance
  - Edge case handling

### Test 7: Payout Wallet Persistence
- **Purpose**: Verify payout_wallet field persists through snapshot
- **Validates**:
  - wallet1 persistence
  - wallet2 persistence
  - split persistence
  - Database → Snapshot → Payment flow

### Test 8: Minimum Amount Validation
- **Purpose**: Verify rejection of amounts below minimum (0.01 SOL)
- **Validates**:
  - Amount validation logic
  - Proper error response
  - Minimum threshold enforcement

## Understanding Test Output

### Success Indicators
- ✅ Green checkmarks indicate passed assertions
- Test count increments in summary

### Failure Indicators
- ❌ Red X marks indicate failed assertions
- Error messages show what went wrong
- Stack traces help debug issues

### Information
- ℹ️ Blue info shows test progress
- ⚠️ Yellow warnings show non-critical issues

## Mock Data Usage

### Mock Solana RPC

The test suite uses `MockSolanaRPC` to simulate blockchain interaction:

```python
from test_helpers import MockSolanaRPC, TestDataGenerator

# Create mock RPC
mock_rpc = MockSolanaRPC()

# Set wallet balance
mock_rpc.set_wallet_balance("GxWallet...", Decimal('0.5'))

# Add mock transaction
tx = TestDataGenerator.create_mock_transaction(
    from_addr="Sender...",
    to_addr="GxWallet...",
    amount_sol=Decimal('0.067567'),
    timestamp=int(time.time())
)
mock_rpc.add_transaction(tx)
```

### Test Data Generator

Generate test data programmatically:

```python
from test_helpers import TestDataGenerator, PaymentScenario

# Generate unique signature
sig = TestDataGenerator.generate_signature()

# Calculate SOL amount
sol_amt = TestDataGenerator.generate_sol_amount(
    base_eur=Decimal('10.0'),
    sol_price_eur=Decimal('150.0'),
    add_offset=True
)

# Create test basket
basket = TestDataGenerator.create_test_basket([
    {'payout_wallet': 'wallet1', 'price': 10.0},
    {'payout_wallet': 'split', 'price': 20.0}
])

# Use pre-configured scenario
scenario = PaymentScenario.scenario_split_payment()
```

## Extending Tests

### Add New Test Scenario

1. Add test method to `TestPaymentSystem` class:

```python
async def test_9_your_scenario(self):
    """Test 9: Your scenario description."""
    print_test_header("Test 9: Your Scenario")
    
    try:
        # Your test logic here
        
        # Assertions
        if condition:
            print_success("Test passed")
            self.passed += 1
        else:
            print_error("Test failed")
            self.failed += 1
            
    except Exception as e:
        print_error(f"Test failed: {e}")
        self.failed += 1
```

2. Call it in `run_all_tests()`:

```python
await tester.test_9_your_scenario()
```

### Add New Mock Scenario

Add to `test_helpers.py` `PaymentScenario` class:

```python
@staticmethod
def scenario_your_case():
    """Scenario: Your case description."""
    return {
        'name': 'Your Case',
        'basket': TestDataGenerator.create_test_basket([
            {'payout_wallet': 'wallet1', 'price': 25.0}
        ]),
        'expected_wallet': 'wallet1',
        'total_eur': Decimal('25.0')
    }
```

## Common Issues

### Import Errors

```
ModuleNotFoundError: No module named 'solders'
```

**Solution**: Tests mock Solana imports. Ensure you're running from project root.

### Database Lock Errors

```
sqlite3.OperationalError: database is locked
```

**Solution**: Close other database connections. Tests use separate `test_bot.db`.

### Test Database Persistence

Tests create `test_bot.db` in the project root. This is automatically cleaned between test runs but you can manually delete it:

```bash
rm test_bot.db
```

## Continuous Integration

To run tests in CI/CD:

```yaml
# Example GitHub Actions workflow
- name: Run Payment Tests
  run: |
    python test_payment_system.py
    if [ $? -ne 0 ]; then
      echo "Tests failed!"
      exit 1
    fi
```

## Coverage

These tests cover:
- ✅ Payment creation logic
- ✅ Wallet routing (wallet1, wallet2, middleman)
- ✅ Split payment detection
- ✅ Mixed basket handling
- ✅ Random offset uniqueness
- ✅ Tolerance calculations
- ✅ Payout wallet persistence
- ✅ Minimum amount validation

**Not covered** (requires integration testing):
- ❌ Actual Solana RPC calls
- ❌ Real blockchain transactions
- ❌ Split forwarding execution (requires funded middleman)
- ❌ Telegram bot integration
- ❌ Concurrent user interactions

## Next Steps

After all tests pass:
1. Deploy to Render
2. Fund middleman wallet (minimum 0.005 SOL)
3. Create test product with split payment
4. Make small test purchase (< 1 EUR)
5. Monitor logs for complete flow
6. Verify split forwarding in blockchain explorers

## Support

If tests fail:
1. Check error messages in test output
2. Review `test_bot.db` for database state
3. Enable debug logging in test scripts
4. Compare with expected behavior in test descriptions
5. Review recent code changes in Phase 1-2

