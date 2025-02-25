# Rust Bitcask Implementation

## Key Design Choices

### 1. Serialization Format: We switched from JSON to Protocol Buffers

- More compact binary format versus human-readable JSON
- Better performance for serialization/deserialization
- Added length prefixes (4-byte) for message boundaries
- Included metadata like timestamps, sequence numbers, and checksums


### 3. Data Integrity: Added checksums for all operations

- Implemented the Checksumable trait for consistent checksum calculation
- Used CRC32 for efficient checksum verification
- Added verification during reads to detect corruption


### 4. Sequence Numbering:

- Added global monotonic sequence numbers to all operations
- Persisted and recovered sequence counters during startup
- Used for establishing total operation order and crash recovery


### 5. Buffering Strategy:

- Configured custom buffer sizes for reading and writing
- Separate buffer sizes for different operations
- Balanced memory usage against performance


### 6. Storage Structure:

- Maintained the log-structured approach with generation numbers
- Added binary format with explicit length prefixes
- Improved position tracking for binary data
