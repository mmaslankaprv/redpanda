#pragma once

#include "storage/api.h"

#include <seastar/core/sstring.hh>

#include <memory>
#include <string_view>
namespace pandoscope {

struct ntp_revision {
    ntp_revision(model::ntp ntp, model::revision_id rev)
      : ntp(std::move(ntp))
      , revision(rev) {}
    model::ntp ntp;
    model::revision_id revision;
};
} // namespace pandoscope
