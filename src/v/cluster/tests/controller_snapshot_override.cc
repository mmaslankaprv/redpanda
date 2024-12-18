#include "cluster/controller_snapshot.h"
#include "storage/snapshot.h"
#include "test_utils/test.h"

TEST(ControllerSnapshotOverride, Override) {
    storage::simple_snapshot_manager manager(
      "/home/mmaslanka/dev/support/controller-overwrite/controller/"
      "0_0",
      "snapshot",
      ss::default_priority_class());
    auto reader = manager.open_snapshot().get();

    size_t size = reader->get_snapshot_size().get();
    fmt::print(">>> {}\n", size);
    auto buf = read_iobuf_exactly(reader->input(), size).get();
    fmt::print(">>> b {}\n", buf.size_bytes());
    auto snap_buf_parser = iobuf_parser{std::move(buf)};

    // auto snapshot
    //   =
    //   serde::read_async<cluster::controller_snapshot>(snap_buf_parser).get();
    // fmt::print(">>> {}\n", snapshot.config.values.size());
    reader->close().get();

    ASSERT_TRUE(false);
}
