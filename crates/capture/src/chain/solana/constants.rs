//! Known Solana program IDs and constants.

// DEX programs
pub const RAYDIUM_AMM_PROGRAM: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
pub const ORCA_WHIRLPOOL_PROGRAM: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
pub const JUPITER_V6_PROGRAM: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";

// System programs
pub const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
pub const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";

// Wrapped SOL mint
pub const WRAPPED_SOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// All DEX program IDs we track.
pub const DEX_PROGRAMS: &[&str] = &[
    RAYDIUM_AMM_PROGRAM,
    ORCA_WHIRLPOOL_PROGRAM,
    JUPITER_V6_PROGRAM,
];

/// Slots per day at ~400ms per slot.
pub const SLOTS_PER_DAY: u64 = 216_000;
