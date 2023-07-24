defmodule WeathermanTest do
  use ExUnit.Case
  doctest Weatherman

  test "greets the world" do
    assert Weatherman.hello() == :world
  end
end
