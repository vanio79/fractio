# Dependency Injection Container for Fractio
# Thread-safe service container with singleton/scoped/transient lifecycles

import std/[locks, hashes, strformat, strutils, options]
import tables

export locks, hashes, strformat, options

# =============================================================================
# Service Lifecycle
# =============================================================================

type
  ServiceLifecycle* = enum
    ## Service instantiation strategy
    slSingleton ## One instance for application lifetime
    slScoped    ## One instance per scope (request, transaction)
    slTransient ## New instance every resolution

# =============================================================================
# Service Entry (stores factory and instance)
# =============================================================================

type
  ServiceEntry* = object
    ## Entry in the service registry
    lifecycle*: ServiceLifecycle
    factory*: proc(): RootRef {.gcsafe.} ## Factory that creates service
    instance*: RootRef                   ## Cached instance for singletons
    isInstantiated*: bool                ## Track if singleton was created

# =============================================================================
# Container
# =============================================================================

type
  Container* = ref object
    ## Thread-safe DI container
    services*: tables.Table[string, ServiceEntry]
      ## Service registry by name
    scopes*: tables.Table[string, RootRef]
      ## Scope-level instances
    currentScope*: string
      ## Current scope identifier
    lock*: Lock
      ## Mutex for thread safety
    parent*: Container
      ## Parent container for hierarchical resolution

# =============================================================================
# Container Operations
# =============================================================================

proc newContainer*(): Container =
  ## Create a new empty container
  result = Container(
    services: tables.initTable[string, ServiceEntry](),
    scopes: tables.initTable[string, RootRef](),
    currentScope: "",
    parent: nil
  )
  initLock(result.lock)

proc close*(c: Container) =
  ## Release container resources
  deinitLock(c.lock)

# =============================================================================
# Registration
# =============================================================================

proc registerService*(c: Container,
                      name: string,
                      lifecycle: ServiceLifecycle,
                      factory: proc(): RootRef {.gcsafe.}) =
  ## Register a service factory with the container
  ## Thread-safe via lock
  withLock(c.lock):
    c.services[name] = ServiceEntry(
      lifecycle: lifecycle,
      factory: factory,
      instance: nil,
      isInstantiated: false
    )

proc registerSingleton*(c: Container,
                        name: string,
                        factory: proc(): RootRef {.gcsafe.}) =
  ## Register a singleton service
  c.registerService(name, slSingleton, factory)

proc registerScoped*(c: Container,
                     name: string,
                     factory: proc(): RootRef {.gcsafe.}) =
  ## Register a scoped service (one instance per scope)
  c.registerService(name, slScoped, factory)

proc registerTransient*(c: Container,
                        name: string,
                        factory: proc(): RootRef {.gcsafe.}) =
  ## Register a transient service (new instance each time)
  c.registerService(name, slTransient, factory)

proc registerInstance*(c: Container, name: string, instance: RootRef) =
  ## Register an existing instance as a singleton
  ## Note: instance must be cast to RootRef if it's a typed ref:
  ##   c.registerInstance("name", cast[RootRef](myTypedRef))
  withLock(c.lock):
    c.services[name] = ServiceEntry(
      lifecycle: slSingleton,
      factory: nil,
      instance: instance,
      isInstantiated: true
    )

# =============================================================================
# Resolution
# =============================================================================

proc resolveRaw*(c: Container, name: string): RootRef =
  ## Resolve a service by name, returns raw RootRef
  ## Caller must cast to appropriate type

  # Check current scope first
  if c.currentScope.len > 0:
    let scopeKey = fmt"{name}#{c.currentScope}"
    withLock(c.lock):
      if scopeKey in c.scopes:
        return c.scopes[scopeKey]

  # Check services registry
  withLock(c.lock):
    if name notin c.services:
      # Try parent container if exists
      if c.parent != nil:
        return c.parent.resolveRaw(name)
      raise newException(KeyError, fmt"Service not registered: {name}")

    var entry = c.services[name]

    case entry.lifecycle
    of slSingleton:
      if not entry.isInstantiated:
        if entry.factory != nil:
          entry.instance = entry.factory()
        entry.isInstantiated = true
        c.services[name] = entry
      return entry.instance

    of slScoped:
      if c.currentScope.len > 0:
        let scopeKey = fmt"{name}#{c.currentScope}"
        if scopeKey notin c.scopes:
          c.scopes[scopeKey] = entry.factory()
        return c.scopes[scopeKey]
      else:
        return entry.factory()

    of slTransient:
      return entry.factory()

proc resolve*[T](c: Container, name: string): T =
  ## Resolve a service by name with type-safe cast
  ## T should be a ref type (RootRef descendant)
  let raw = c.resolveRaw(name)
  when T is ref:
    result = cast[T](raw)
  else:
    {.error: "Container.resolve requires a ref type".}

proc tryResolve*[T](c: Container, name: string): Option[T] =
  ## Try to resolve a service, returns none if not found
  try:
    some(resolve[T](c, name))
  except KeyError:
    none(T)

# =============================================================================
# Scope Management
# =============================================================================

proc beginScope*(c: Container, scopeId: string) =
  ## Begin a new scope for scoped services
  withLock(c.lock):
    c.currentScope = scopeId

proc endScope*(c: Container) =
  ## End current scope, release scoped instances
  withLock(c.lock):
    if c.currentScope.len > 0:
      var keysToRemove: seq[string] = @[]
      for key in c.scopes.keys:
        if key.endsWith(fmt"#{c.currentScope}"):
          keysToRemove.add(key)
      for key in keysToRemove:
        c.scopes.del(key)
      c.currentScope = ""

# =============================================================================
# Service Checking
# =============================================================================

proc hasService*(c: Container, name: string): bool =
  ## Check if a service is registered
  withLock(c.lock):
    result = name in c.services
    if not result and c.parent != nil:
      result = c.parent.hasService(name)

proc getServiceNames*(c: Container): seq[string] =
  ## Get all registered service names
  withLock(c.lock):
    result = @[]
    for name in c.services.keys:
      result.add(name)

proc getLifecycle*(c: Container, name: string): ServiceLifecycle =
  ## Get lifecycle of a registered service
  withLock(c.lock):
    if name in c.services:
      result = c.services[name].lifecycle
    elif c.parent != nil:
      result = c.parent.getLifecycle(name)
    else:
      raise newException(KeyError, fmt"Service not registered: {name}")

# =============================================================================
# Hierarchical Container Support
# =============================================================================

proc createChildContainer*(c: Container): Container =
  ## Create a child container that inherits from parent
  result = newContainer()
  result.parent = c

proc overrideService*(c: Container, name: string, factory: proc(): RootRef {.gcsafe.}) =
  ## Override a parent service in child container
  c.registerService(name, slSingleton, factory)

# =============================================================================
# Container Builder (Convenience API)
# =============================================================================

type
  ContainerBuilder* = ref object
    ## Builder for convenient container construction
    container*: Container

proc newContainerBuilder*(): ContainerBuilder =
  ## Create a new container builder
  result = ContainerBuilder(container: newContainer())

proc addSingleton*[T](b: ContainerBuilder,
                      name: string,
                      factory: proc(): T {.gcsafe.}): ContainerBuilder =
  ## Add a singleton service to builder
  b.container.registerSingleton(name,
    proc(): RootRef {.gcsafe.} = cast[RootRef](factory()))
  result = b

proc addScoped*[T](b: ContainerBuilder,
                   name: string,
                   factory: proc(): T {.gcsafe.}): ContainerBuilder =
  ## Add a scoped service to builder
  b.container.registerScoped(name,
    proc(): RootRef {.gcsafe.} = cast[RootRef](factory()))
  result = b

proc addTransient*[T](b: ContainerBuilder,
                      name: string,
                      factory: proc(): T {.gcsafe.}): ContainerBuilder =
  ## Add a transient service to builder
  b.container.registerTransient(name,
    proc(): RootRef {.gcsafe.} = cast[RootRef](factory()))
  result = b

proc addInstance*(b: ContainerBuilder, name: string,
    instance: RootRef): ContainerBuilder =
  ## Add an existing instance as singleton
  ## Caller must cast ref type to RootRef: cast[RootRef](myInstance)
  b.container.registerInstance(name, instance)
  result = b

proc build*(b: ContainerBuilder): Container =
  ## Build the final container
  result = b.container
