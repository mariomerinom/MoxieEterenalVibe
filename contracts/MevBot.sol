// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

/// @title MevBot - Atomic sandwich and arbitrage execution
/// @notice Owner-only contract for MEV extraction via Flashbots bundles.
///         All operations revert atomically if unprofitable.
/// @dev Deploy once, fund with ETH, execute via signed bundles.

interface IUniswapV2Pair {
    function swap(uint amount0Out, uint amount1Out, address to, bytes calldata data) external;
    function getReserves() external view returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast);
    function token0() external view returns (address);
    function token1() external view returns (address);
}

interface IUniswapV3Pool {
    function swap(
        address recipient,
        bool zeroForOne,
        int256 amountSpecified,
        uint160 sqrtPriceLimitX96,
        bytes calldata data
    ) external returns (int256 amount0, int256 amount1);
    function token0() external view returns (address);
    function token1() external view returns (address);
    function fee() external view returns (uint24);
}

interface IERC20 {
    function balanceOf(address) external view returns (uint256);
    function transfer(address to, uint256 amount) external returns (bool);
    function approve(address spender, uint256 amount) external returns (bool);
}

interface IWETH {
    function deposit() external payable;
    function withdraw(uint256 amount) external;
    function balanceOf(address) external view returns (uint256);
    function transfer(address to, uint256 amount) external returns (bool);
    function approve(address spender, uint256 amount) external returns (bool);
}

interface IFlashLoanReceiver {
    function onFlashLoan(
        address initiator,
        address token,
        uint256 amount,
        uint256 fee,
        bytes calldata data
    ) external returns (bytes32);
}

contract MevBot {
    address public immutable owner;
    address public constant WETH = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2;

    // V3 price limits (min/max sqrt price for swaps)
    uint160 internal constant MIN_SQRT_RATIO = 4295128739;
    uint160 internal constant MAX_SQRT_RATIO = 1461446703485210103287273052203988822378723970342;

    modifier onlyOwner() {
        require(msg.sender == owner, "not owner");
        _;
    }

    constructor() {
        owner = msg.sender;
    }

    receive() external payable {}

    // -----------------------------------------------------------------------
    // V2 Sandwich
    // -----------------------------------------------------------------------

    /// @notice Execute a V2 swap (one leg of a sandwich).
    /// @param pair The Uniswap V2 pair contract.
    /// @param tokenIn The token we're sending.
    /// @param amountIn How much tokenIn to send.
    /// @param amountOutMin Minimum output (slippage protection).
    /// @param zeroForOne True if tokenIn is token0, false if token1.
    function swapV2(
        address pair,
        address tokenIn,
        uint256 amountIn,
        uint256 amountOutMin,
        bool zeroForOne
    ) external onlyOwner {
        // Transfer tokenIn to pair
        IERC20(tokenIn).transfer(pair, amountIn);

        // Calculate output using constant product formula
        (uint112 r0, uint112 r1,) = IUniswapV2Pair(pair).getReserves();
        uint256 reserveIn;
        uint256 reserveOut;
        if (zeroForOne) {
            reserveIn = r0;
            reserveOut = r1;
        } else {
            reserveIn = r1;
            reserveOut = r0;
        }

        uint256 amountInWithFee = amountIn * 997;
        uint256 amountOut = (amountInWithFee * reserveOut) / (reserveIn * 1000 + amountInWithFee);
        require(amountOut >= amountOutMin, "slippage");

        if (zeroForOne) {
            IUniswapV2Pair(pair).swap(0, amountOut, address(this), "");
        } else {
            IUniswapV2Pair(pair).swap(amountOut, 0, address(this), "");
        }
    }

    // -----------------------------------------------------------------------
    // V3 Sandwich
    // -----------------------------------------------------------------------

    /// @notice Execute a V3 swap (one leg of a sandwich).
    /// @param pool The Uniswap V3 pool contract.
    /// @param zeroForOne Direction: true = token0 in / token1 out.
    /// @param amountIn Amount of input token (positive = exact input).
    function swapV3(
        address pool,
        bool zeroForOne,
        int256 amountIn
    ) external onlyOwner {
        uint160 sqrtPriceLimit = zeroForOne ? MIN_SQRT_RATIO + 1 : MAX_SQRT_RATIO - 1;
        IUniswapV3Pool(pool).swap(
            address(this),
            zeroForOne,
            amountIn,
            sqrtPriceLimit,
            ""
        );
    }

    /// @notice V3 swap callback — transfers tokens to the pool.
    function uniswapV3SwapCallback(
        int256 amount0Delta,
        int256 amount1Delta,
        bytes calldata
    ) external {
        // Pay the pool whatever it asks for
        if (amount0Delta > 0) {
            address token0 = IUniswapV3Pool(msg.sender).token0();
            IERC20(token0).transfer(msg.sender, uint256(amount0Delta));
        }
        if (amount1Delta > 0) {
            address token1 = IUniswapV3Pool(msg.sender).token1();
            IERC20(token1).transfer(msg.sender, uint256(amount1Delta));
        }
    }

    // -----------------------------------------------------------------------
    // Multi-hop Arbitrage
    // -----------------------------------------------------------------------

    /// @notice Execute a multi-hop arb through V2 and/or V3 pools.
    /// @param hops Encoded array of (pool, isV3, zeroForOne) tuples.
    /// @param tokenIn Starting token.
    /// @param amountIn Starting amount.
    /// @param minProfit Minimum profit in tokenIn units (reverts if not met).
    function executeArb(
        bytes calldata hops,
        address tokenIn,
        uint256 amountIn,
        uint256 minProfit
    ) external onlyOwner {
        uint256 balanceBefore = IERC20(tokenIn).balanceOf(address(this));

        uint256 currentAmount = amountIn;
        uint256 offset = 0;

        while (offset < hops.length) {
            // Decode hop: 20 bytes pool + 1 byte isV3 + 1 byte zeroForOne = 22 bytes
            address pool = address(bytes20(hops[offset:offset+20]));
            bool isV3 = uint8(hops[offset+20]) == 1;
            bool zeroForOne = uint8(hops[offset+21]) == 1;
            offset += 22;

            if (isV3) {
                // V3 swap — callback handles token transfer
                address token0 = IUniswapV3Pool(pool).token0();
                address currentToken = zeroForOne ? token0 : IUniswapV3Pool(pool).token1();

                // Approve pool to pull tokens via callback
                uint160 sqrtPriceLimit = zeroForOne ? MIN_SQRT_RATIO + 1 : MAX_SQRT_RATIO - 1;
                (int256 d0, int256 d1) = IUniswapV3Pool(pool).swap(
                    address(this),
                    zeroForOne,
                    int256(currentAmount),
                    sqrtPriceLimit,
                    ""
                );

                // Output amount is the negative delta
                currentAmount = uint256(-(zeroForOne ? d1 : d0));
            } else {
                // V2 swap
                address currentToken;
                if (zeroForOne) {
                    currentToken = IUniswapV2Pair(pool).token0();
                } else {
                    currentToken = IUniswapV2Pair(pool).token1();
                }

                // Transfer tokens to pair
                IERC20(currentToken).transfer(pool, currentAmount);

                (uint112 r0, uint112 r1,) = IUniswapV2Pair(pool).getReserves();
                uint256 reserveIn = zeroForOne ? r0 : r1;
                uint256 reserveOut = zeroForOne ? r1 : r0;

                uint256 amountInWithFee = currentAmount * 997;
                currentAmount = (amountInWithFee * reserveOut) / (reserveIn * 1000 + amountInWithFee);

                if (zeroForOne) {
                    IUniswapV2Pair(pool).swap(0, currentAmount, address(this), "");
                } else {
                    IUniswapV2Pair(pool).swap(currentAmount, 0, address(this), "");
                }
            }
        }

        uint256 balanceAfter = IERC20(tokenIn).balanceOf(address(this));
        require(balanceAfter >= balanceBefore + minProfit, "not profitable");
    }

    // -----------------------------------------------------------------------
    // Builder tip
    // -----------------------------------------------------------------------

    /// @notice Pay the block builder (coinbase) a tip.
    /// @dev Call this at the end of a profitable bundle.
    function payBuilder(uint256 amount) external onlyOwner {
        (bool ok,) = block.coinbase.call{value: amount}("");
        require(ok, "tip failed");
    }

    /// @notice Pay the builder a percentage of profit.
    /// @param profitWei Total profit in wei.
    /// @param bribePercent Percentage to pay (e.g., 90 = 90%).
    function payBuilderPercent(uint256 profitWei, uint256 bribePercent) external onlyOwner {
        uint256 tip = (profitWei * bribePercent) / 100;
        (bool ok,) = block.coinbase.call{value: tip}("");
        require(ok, "tip failed");
    }

    // -----------------------------------------------------------------------
    // WETH helpers
    // -----------------------------------------------------------------------

    /// @notice Wrap ETH to WETH.
    function wrapETH(uint256 amount) external onlyOwner {
        IWETH(WETH).deposit{value: amount}();
    }

    /// @notice Unwrap WETH to ETH.
    function unwrapWETH(uint256 amount) external onlyOwner {
        IWETH(WETH).withdraw(amount);
    }

    // -----------------------------------------------------------------------
    // Admin
    // -----------------------------------------------------------------------

    /// @notice Withdraw ETH to owner.
    function withdrawETH() external onlyOwner {
        (bool ok,) = owner.call{value: address(this).balance}("");
        require(ok, "withdraw failed");
    }

    /// @notice Withdraw any ERC20 to owner.
    function withdrawToken(address token) external onlyOwner {
        uint256 bal = IERC20(token).balanceOf(address(this));
        IERC20(token).transfer(owner, bal);
    }

    /// @notice Approve a token for a spender (needed for some DEX interactions).
    function approveToken(address token, address spender, uint256 amount) external onlyOwner {
        IERC20(token).approve(spender, amount);
    }
}
