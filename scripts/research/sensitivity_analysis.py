#!/usr/bin/env python3
"""Quick sensitivity analysis: profit vs swap size and capture rate."""

# Profitable pools from the per-pool estimator output
# (name, freq/day, liquidity_eth, profit_at_1eth_swap)
pools = [
    ("USDC/WETH 0x397ff", 216, 57.8, 0.01541),
    ("USDC/WETH 0x2e813", 334, 60.2, 0.01435),
    ("USDC/WETH 0x3aa37", 200, 114.8, 0.00743),
    ("WETH/USDT 0x17c1a", 642, 62.5, 0.01254),
    ("DAI/WETH  0xabb09", 746, 78.1, 0.01124),
    ("???/WETH  0xb771f", 55, 8.3, 0.09271),
    ("???/WETH  0x74141", 58, 4.3, 0.02775),
    ("???/WETH  0xc0a6b", 507, 79.1, 0.00606),
    ("???/WETH  0x9e090", 260, 57.0, 0.00666),
]

gas = 150000 * 30e-9  # 0.0045 ETH
sizes = [0.1, 0.3, 0.5, 1.0, 2.0, 5.0]

print("=" * 100)
print("  SENSITIVITY: Daily Net ETH by Pool, Swap Size")
print("  (Quadratic scaling from 1 ETH baseline, 100% capture)")
print("=" * 100)

header = f"{'Pool':>25}  {'Freq/d':>6}  {'Liq':>6}"
for s in sizes:
    header += f"  {'@'+str(s)+'E':>8}"
print(header)

total_by_size = {s: 0 for s in sizes}

for name, freq, liq, p1 in pools:
    row = f"{name:>25}  {freq:>6}  {liq:>6.1f}"
    for s in sizes:
        # Profit scales roughly as swap_size^2 for small swaps
        # This comes from: price_impact ~ swap/liq, arb_profit ~ impact * arb_size
        scaled = p1 * (s ** 2)
        net = max(scaled - gas, 0) * freq
        total_by_size[s] += net
        row += f"  {net:>8.2f}"
    print(row)

print()
row = f"{'TOTAL NET ETH/day':>25}  {'':>6}  {'':>6}"
for s in sizes:
    row += f"  {total_by_size[s]:>8.2f}"
print(row)

print()
print("=" * 100)
print("  REVENUE MATRIX: Daily USD at various (swap_size, capture_rate)")
print("  ETH price = $2,500")
print("=" * 100)

print(f"\n{'':>15}", end="")
for cr in [1.0, 0.50, 0.10, 0.05, 0.01]:
    print(f"  {'CR='+str(int(cr*100))+'%':>12}", end="")
print()

for s in sizes:
    net = total_by_size[s]
    row = f"  Swap={s:>4.1f} ETH"
    for cr in [1.0, 0.50, 0.10, 0.05, 0.01]:
        usd = net * cr * 2500
        row += f"  ${usd:>10,.0f}"
    print(row)

print()
print("NOTES:")
print("  - Profit scaling is approximate (quadratic in swap size)")
print("  - Only pools with measurable profit at 1 ETH are included")
print("  - Large V3 pools (>1000 ETH liq) show 0 profit — excluded")
print("  - Gas: 150k gas @ 30 gwei = 0.0045 ETH per attempt")
print("  - Capture rate = what fraction of opportunities we win")
print()
print("KEY QUESTION: What's the actual average swap size on")
print("these small pools? If it's 0.1 ETH → strategy is dead.")
print("If it's 0.5-1.0 ETH → strategy may hit $1K/day target.")
print("MEV-Share hints don't reveal swap amounts, so we need")
print("to check on-chain data for these specific pools.")
