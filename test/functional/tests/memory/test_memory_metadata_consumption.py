#
# Copyright(c) 2021 Intel Corporation
# SPDX-License-Identifier: BSD-3-Clause-Clear
#

import pytest

from api.cas import casadm
from api.cas.cache import Cache
from api.cas.cache_config import CacheLineSize, CacheMode, SeqCutOffPolicy
from core.test_run import TestRun
from storage_devices.disk import DiskType, DiskTypeSet, DiskTypeLowerThan
from test_tools.fio.fio import Fio
from test_tools.fio.fio_param import ReadWrite, IoEngine
from test_utils.os_utils import get_mem_available, get_metadata_size
from test_utils.os_utils import sync, Udev
from test_utils.size import Size, Unit

cores_count = 16
cache_mode_wb = CacheMode.WB
percentage_tolerance = 5
cas_cache_module = 'cas_cache'


@pytest.mark.parametrizex("cache_mode", CacheMode)
@pytest.mark.parametrizex("cache_line_size", CacheLineSize)
@pytest.mark.require_disk("cache", DiskTypeSet([DiskType.optane, DiskType.nand]))
@pytest.mark.require_disk("core", DiskTypeLowerThan("cache"))
def test_memory_metadata_consumption_empty_cache(cache_line_size, cache_mode):
    """
        title: Test for CAS RAM consumption for metadata.
        description: |
          Validate CAS RAM consumption for metadata depends on start command parameters.
        pass_criteria:
          - Successful CAS creation and cores addition.
          - Memory consumption within limits.
    """
    with TestRun.step("Prepare drives and partitions for core devices."):
        cache_disk = TestRun.disks['cache']
        cache_disk.create_partitions([Size(50, Unit.GibiByte)])
        cache_dev = cache_disk.partitions[0]

        core_disk = TestRun.disks['core']
        parts = [Size(3200, Unit.MebiByte)] * cores_count
        core_disk.create_partitions(parts)
        core_devices = core_disk.partitions

    with TestRun.step("Start cache with configuration and add all core devices."):
        free_mem_before_start = get_mem_available()

        cache = casadm.start_cache(cache_dev, cache_mode, cache_line_size, force=True)

        for dev in core_devices:
            cache.add_core(dev)

    with TestRun.step("Measure allocates size of metadata."):
        used_memory = get_metadata_size(cas_cache_module)
        TestRun.LOGGER.info(f"Memory allocated: {used_memory}")

    with TestRun.step("Compare allocated size with cache usage statistics "
                      "and required consumption."):
        free_mem_after_start = get_mem_available()

        metadata_max_size = Cache.get_metadata_max_size(cache.size, cache_line_size)
        TestRun.LOGGER.info(f"Metadata max size: {metadata_max_size}")

        validate_memory_consumption(cache, metadata_max_size, used_memory)

        memory_delta = free_mem_before_start - free_mem_after_start
        ram_change_status = is_memory_within_limit("Measured increment of RAM usage after start",
                                                   metadata_max_size, memory_delta)

        TestRun.LOGGER.info("Stop CAS and verify memory changes again")
        casadm.stop_all_caches()

        free_mem_after_stop = get_mem_available()
        memory_delta = free_mem_after_stop - free_mem_after_start
        ram_change_status |= is_memory_within_limit("Measured decrement of RAM usage after stop",
                                                    metadata_max_size, memory_delta)

        if ram_change_status:
            TestRun.LOGGER.info(f"Memory measurements completed with correct results.")
        else:
            TestRun.LOGGER.error(f"Memory measurements completed, found problems.")


@pytest.mark.parametrizex("cache_line_size", CacheLineSize)
@pytest.mark.require_disk("cache", DiskTypeSet([DiskType.optane, DiskType.nand]))
@pytest.mark.require_disk("core", DiskTypeLowerThan("cache"))
def test_memory_metadata_consumption_full_cache(cache_line_size):
    """
        title: Test for CAS RAM consumption for metadata for cache usage 100%.
        description: |
          Validate CAS RAM consumption for metadata depends on start cache line size
          for cache usage 100% (with 5% tolerance) where cache mode Write-Back is used.
        pass_criteria:
          - Successful CAS creation and cores addition.
          - Filling all cache space successfully.
          - Memory consumption within limits.
    """
    with TestRun.step("Prepare drives and partitions for core devices."):
        cache_disk = TestRun.disks['cache']
        cache_disk.create_partitions([Size(50, Unit.GibiByte)])
        cache_dev = cache_disk.partitions[0]

        core_disk = TestRun.disks['core']
        parts = [Size(3200, Unit.MebiByte)] * cores_count
        core_disk.create_partitions(parts)
        core_devices = core_disk.partitions

    with TestRun.step("Start cache with configuration and add all core devices."):
        free_mem_before_start = get_mem_available()

        cache = casadm.start_cache(cache_dev, cache_mode_wb, cache_line_size, force=True)
        Udev.disable()
        # make sure that I/O will go to the cache
        cache.set_seq_cutoff_policy(SeqCutOffPolicy.never)

        core_list = []
        for dev in core_devices:
            core_list.append(cache.add_core(dev))

    with TestRun.step("Fill all cache space - run Fio instance on each of cores"):
        sync()
        fio = (Fio().create_command()
               .direct()
               .io_engine(IoEngine.libaio)
               .size(cache.cache_device.size)
               .read_write(ReadWrite.read)
               .io_depth(64)
               .block_size(Size(1, Unit.Blocks4096)))
        for dev in core_list:
            fio.add_job(f"job_{dev.core_id}").target(dev.path)
        fio.run()

        occupancy = cache.get_statistics(percentage_val=True).usage_stats.occupancy
        TestRun.LOGGER.info(f"Cache occupancy: {occupancy}")
        if occupancy < (100 - percentage_tolerance):
            TestRun.LOGGER.warning(f"Cache occupancy is below expectation.")

    with TestRun.step("Measure allocates size of metadata."):
        used_memory = get_metadata_size(cas_cache_module)
        TestRun.LOGGER.info(f"Memory allocated: {used_memory}")

    with TestRun.step("Compare allocated size with cache usage statistics "
                      "and required consumption."):
        free_mem_after_start = get_mem_available()

        metadata_max_size = Cache.get_metadata_max_size(cache.size, cache_line_size)
        TestRun.LOGGER.info(f"Metadata max size: {metadata_max_size}")

        validate_memory_consumption(cache, metadata_max_size, used_memory)

        memory_delta = free_mem_before_start - free_mem_after_start
        ram_change_status = is_memory_within_limit("Measured increment of RAM usage after start",
                                                   metadata_max_size, memory_delta)

        TestRun.LOGGER.info("Stop CAS and verify memory changes again")
        casadm.stop_all_caches()

        free_mem_after_stop = get_mem_available()
        memory_delta = free_mem_after_stop - free_mem_after_start
        ram_change_status |= is_memory_within_limit("Measured decrement of RAM usage after stop",
                                                    metadata_max_size, memory_delta)

        if ram_change_status:
            TestRun.LOGGER.info(f"Memory measurements completed with correct results.")
        else:
            TestRun.LOGGER.error(f"Memory measurements completed, found problems.")


def is_memory_within_limit(value_name, expected, used):
    """Check if size of used memory is within expected maximum size"""
    if expected >= used:
        TestRun.LOGGER.info(f"{value_name} ({used}) is within limit ({expected}).")
        return True
    else:
        TestRun.LOGGER.error(f"{value_name} ({used}) exceeds limit ({expected}).")
        return False


def validate_memory_consumption(cache_device, expected_maximum, actual_size):
    """Checks memory consumption"""
    stats = cache_device.get_statistics()
    print(f"Statistics: {stats}")

    stat_footprint = Size(int(cache_device.get_metadata_size()))
    TestRun.LOGGER.info(f"Allowed limit for current configuration is: {expected_maximum}")

    is_memory_within_limit("Metadata Memory Footprint", expected_maximum, stat_footprint)
    is_memory_within_limit("Allocated memory", expected_maximum, actual_size)
