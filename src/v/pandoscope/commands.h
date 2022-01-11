#pragma once

#include "model/fundamental.h"
#include "pandoscope/executor.h"
#include "raft/types.h"

namespace pandoscope {

pandoscope::executor::command reset_raft_kvstore_config(model::ntp);

}
