#pragma once

#include "model/record.h"
#include "pandoscope/executor.h"
#include "storage/log.h"

namespace pandoscope {

ss::future<> print(storage::log);

executor::command print_command();

} // namespace pandoscope
