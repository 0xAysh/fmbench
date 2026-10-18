import unittest

from fmbench import meta


class NodeFlagsTest(unittest.TestCase):
    def test_drops_the_binary_and_the_filter_maps_flags(self):
        cmdline = ["/data/bin/reth", "node", "--chain", "mainnet", "--filter-maps", "--filter-maps.history", "0",
                   "--filter-maps.history=0", "--filter-maps=true", "--engine.persistence-threshold", "0"]
        self.assertEqual(meta.node_flags(cmdline), ["node", "--chain", "mainnet", "--engine.persistence-threshold", "0"])

    def test_the_modes_of_one_setup_have_the_same_flags(self):
        base = ["/data/bin/reth", "node", "--metrics", "127.0.0.1:9001"]
        self.assertEqual(meta.node_flags(base + ["--filter-maps"]), meta.node_flags(["/data/bin/reth-2.7.0"] + base[1:]))
