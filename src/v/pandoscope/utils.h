#pragma once

#include "storage/log.h"

namespace pandoscope {

static ss::future<model::record_batch_reader>
read_all_reader(storage::log log) {
    storage::log_reader_config cfg(
      log.offsets().start_offset,
      log.offsets().dirty_offset,
      ss::default_priority_class());

    return log.make_reader(cfg);
}

} // namespace pandoscope
