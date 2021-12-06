#pragma once

enum class format_tag {
    batch_header_crc,
    batch_size,
    batch_base_offset,
    batch_type,
    batch_crc,
    batch_attributes,
    batch_compression,
    batch_ts_type,
    batch_transactional,
    batch_control,
    batch_last_offset_delta,
    batch_first_ts,
    batch_last_ts,
    batch_producer_epoch,
    batch_base_sequence,
    batch_record_count,
};

class header_crc_formatter {
    static constexpr const char* tag = "%c";
};
