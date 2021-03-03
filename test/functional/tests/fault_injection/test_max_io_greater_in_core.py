#
# Copyright(c) 2021 Intel Corporation
# SPDX-License-Identifier: BSD-3-Clause-Clear
#
import pytest

from api.cas import casadm, cas_module
from api.cas.cache_config import CacheMode, CacheLineSize
from core.test_run import TestRun
from storage_devices.disk import DiskType, DiskTypeSet
from test_tools.dd import Dd
from test_utils import os_utils
from test_utils.size import Size, Unit


@pytest.mark.require_disk("cache", DiskTypeSet([DiskType.optane, DiskType.nand]))
@pytest.mark.parametrizex("module", cas_module.CasModule)
@pytest.mark.parametrizex("cache_mode", CacheMode)
@pytest.mark.parametrizex("cache_line_size", CacheLineSize)
@pytest.mark.require_plugin("scsi_debug", delay="0", virtual_gb="4", dev_size_mb="500",
                            sector_size="512", physblk_exp="4")
def test_max_io_greater_in_core(module, cache_mode, cache_line_size):
    """
        title: Test behavior when core's max IO (max_sectors_kb) is greater than cache's.
        description: |
          Test behavior when core's max IO (max_sectors_kb) is greater than cache' for
          various CAS configurations.
        pass_criteria:
          - No kernel bug.
          - Running workload successfully.
    """
    with TestRun.step("Prepare devices."):
        core_scsi_debug_disk = TestRun.scsi_debug_devices[0]

        cache_disk = TestRun.disks['cache']
        cache_disk.create_partitions([Size(1, Unit.GibiByte)])
        cache_dev = cache_disk.partitions[0]

    with TestRun.step("Set 'max_sectors_kb' for core device"):
        max_hw_io_size = core_scsi_debug_disk.get_max_hw_io_size()
        new_max_io_size = min(max_hw_io_size, Size(4, Unit.MebiByte))
        core_scsi_debug_disk.set_max_io_size(new_max_io_size)

    with TestRun.step("Check if max_sectors_kb for cache device is less than core's"):
        core_max_io = core_scsi_debug_disk.get_max_io_size()
        cache_max_io = cache_disk.get_max_io_size()
        TestRun.LOGGER.info(f"Cache max io size: {cache_max_io}, Core max io size: {core_max_io}")

        if core_max_io <= cache_max_io or core_max_io < Size(1000, Unit.KiloByte):
            TestRun.LOGGER.fail("Could not set core or cache max_sectors_kb limit - "
                                "Assumption for core.max_io > cache.max_io & core.max_io > 1MiB. "
                                f"core.max_io value is {core_max_io}")

    with TestRun.step("Unload CAS modules if needed"):
        if os_utils.is_kernel_module_loaded(module.value):
            cas_module.unload_all_cas_modules()

    with TestRun.step(f"Loading CAS modules"):
        os_utils.load_kernel_module(cas_module.CasModule.cache.value)

    with TestRun.step("Starting cache"):
        cache = casadm.start_cache(cache_dev, cache_mode, cache_line_size, force=True)

    with TestRun.step("Adding core"):
        core = cache.add_core(core_scsi_debug_disk)

    with TestRun.step("Run IO"):
        block_size_multipliers = [1, 2, 4, 8]
        seek_bytes = Size(0, Unit.Byte)

        for block_size_multiplier in block_size_multipliers:

            block_size = core_max_io * block_size_multiplier

            TestRun.LOGGER.info(f"Testing with block size: {block_size}")
            dd = Dd().input("/dev/zero") \
                .output(core) \
                .count(20) \
                .block_size(block_size) \
                .oflag("direct") \
                .seek(seek_bytes / block_size)
            dd.run()

            seek_bytes += block_size * 20   # each iteration will cover different disk part

            casadm.stop_cache(cache.cache_id)
            casadm.load_cache(cache_dev)
