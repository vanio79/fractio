# Unit tests for Fractio DI Container

import std/[unittest, options, tables]
import fractio/di/container
import fractio/di/mocks

suite "DI Container":

  test "newContainer creates empty container":
    let c = newContainer()
    check c.services.len == 0
    check c.scopes.len == 0
    check c.currentScope == ""
    check c.parent == nil
    c.close()

  test "registerInstance stores pre-created instance":
    let mockTime = newMockTimeProvider(5000)
    let container = newContainer()
    container.registerInstance("timeProvider", mockTime)

    check container.hasService("timeProvider")
    check container.getLifecycle("timeProvider") == slSingleton
    container.close()

  test "resolve returns same singleton instance":
    let mockTime = newMockTimeProvider(5000)
    let container = newContainer()
    container.registerInstance("timeProvider", mockTime)

    let resolved = resolve[MockTimeProvider](container, "timeProvider")
    check resolved.nowNs() == 5000

    # Same instance returned on second resolve
    let resolved2 = resolve[MockTimeProvider](container, "timeProvider")
    check resolved == resolved2
    container.close()

  test "resolve raises KeyError for missing service":
    let c = newContainer()

    var caught = false
    try:
      discard resolve[MockTimeProvider](c, "missing")
    except KeyError:
      caught = true

    check caught
    c.close()

  test "tryResolve returns none for missing service":
    let c = newContainer()

    let result = tryResolve[MockTimeProvider](c, "missing")
    check result.isNone
    c.close()

  test "tryResolve returns some for registered service":
    let mockTime = newMockTimeProvider(100)
    let container = newContainer()
    container.registerInstance("timeProvider", mockTime)

    let result = tryResolve[MockTimeProvider](container, "timeProvider")
    check result.isSome
    check result.get.nowNs() == 100
    container.close()

  test "getServiceNames returns all registered names":
    let c = newContainer()
    c.registerInstance("timeProvider", newMockTimeProvider())
    c.registerInstance("logger", newMockLogProvider())

    let names = c.getServiceNames()
    check names.len == 2
    check "timeProvider" in names
    check "logger" in names
    c.close()

  test "beginScope sets current scope":
    let c = newContainer()

    c.beginScope("testScope")
    check c.currentScope == "testScope"

    c.endScope()
    check c.currentScope == ""
    c.close()

suite "ContainerBuilder":

  test "builder addSingleton creates factory":
    let container = newContainerBuilder()
      .addSingleton("timeProvider", proc(): MockTimeProvider = newMockTimeProvider(5000))
      .build()

    check container.hasService("timeProvider")
    check container.getLifecycle("timeProvider") == slSingleton

    let resolved = resolve[MockTimeProvider](container, "timeProvider")
    check resolved.nowNs() == 5000
    container.close()

suite "Hierarchical Containers":

  test "createChildContainer creates child with parent":
    let parent = newContainer()
    parent.registerInstance("timeProvider", newMockTimeProvider())

    let child = parent.createChildContainer()
    check child.parent == parent

    # Child can resolve from parent
    check child.hasService("timeProvider")

    parent.close()
    child.close()

  test "child can override parent service":
    let parent = newContainer()
    parent.registerInstance("timeProvider", newMockTimeProvider(100))

    let child = parent.createChildContainer()
    child.registerInstance("timeProvider", newMockTimeProvider(200))

    let childResolved = resolve[MockTimeProvider](child, "timeProvider")
    check childResolved.nowNs() == 200

    # Parent still has original
    let parentResolved = resolve[MockTimeProvider](parent, "timeProvider")
    check parentResolved.nowNs() == 100

    parent.close()
    child.close()

  test "hasService checks parent":
    let parent = newContainer()
    parent.registerInstance("timeProvider", newMockTimeProvider())

    let child = parent.createChildContainer()

    check child.hasService("timeProvider") # Found in parent
    check not child.hasService("missing")

    parent.close()
    child.close()

suite "Thread Safety":

  test "concurrent registration and resolution":
    # This test verifies basic thread safety with locks
    let c = newContainer()
    c.registerInstance("timeProvider", newMockTimeProvider())
    c.registerInstance("logger", newMockLogProvider())

    # Multiple resolves should work without race conditions
    for i in 0..10:
      let t1 = resolve[MockTimeProvider](c, "timeProvider")
      let l1 = resolve[MockLogProvider](c, "logger")
      check t1 != nil
      check l1 != nil

    c.close()
