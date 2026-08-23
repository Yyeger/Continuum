defmodule Continuum.Cluster.DurableTermColdNodeTest do
  use Continuum.Test.ClusterCase, async: false

  test "safe decoding loads atoms from deployed but unloaded application modules" do
    peer = start_peer!(:durable_term_cold)

    try do
      fixture = Continuum.Test.DurableTermColdFixture
      assert false == peer_call(peer, :code, :is_loaded, [fixture])

      name = "continuum_durable_term_cold_fixture_atom_v080"
      encoded = <<131, 119, byte_size(name), name::binary>>

      assert :continuum_durable_term_cold_fixture_atom_v080 ==
               peer_call(peer, Continuum.DurableTerm, :decode!, [encoded])

      assert {:file, _path} = peer_call(peer, :code, :is_loaded, [fixture])
    after
      stop_peer(peer)
    end
  end
end
