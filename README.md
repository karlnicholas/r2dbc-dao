This project is no longer being actively maintained.

# Reactive Relational Database Connectivity DAO
This project is an exploration of what a Java API for relational database access with [Reactive Streams][rs] might look like.  It uses [Project Reactor][pr].

[pr]: https://projectreactor.io
[rs]: https://www.reactive-streams.org

## Examples
A quick example of configuration and execution would look like:

```java
  private final R2dbcDao dao;

  // -----------------------------------------------------------------------
  // Establish a connection pool
  // -----------------------------------------------------------------------
  public SomeEntityDao() {
    ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(DRIVER, H2_DRIVER)
        .option(PASSWORD, "")
        .option(URL, "mem:test;DB_CLOSE_DELAY=-1")
        .option(USER, "sa")
        .build());

    ConnectionPoolConfiguration configuration = ConnectionPoolConfiguration.builder(connectionFactory)
        .maxIdleTime(Duration.ofMinutes(30))
        .initialSize(2)
        .maxSize(10)
        .build();

    this.dao = new R2dbcDao(new ConnectionPool(configuration));
  }

  // -----------------------------------------------------------------------
  // Public Facades
  // -----------------------------------------------------------------------
  public Mono<SomeEntity> save(SomeEntity entity) {
    return dao.withConnection(conn -> save(conn, entity)).next();
  }

  private Mono<SomeEntity> findById(Connection conn, Long id) {
    return dao.select(conn, "SELECT id, svalue FROM some_entity WHERE id = $1", mapper, id)
        .next();
  }

  public Flux<SomeEntity> findAll() {
    return dao.select("SELECT id, svalue FROM some_entity", mapper);
  }

  // -----------------------------------------------------------------------
  // Composable Helpers
  // -----------------------------------------------------------------------
  private Mono<SomeEntity> save(Connection conn, SomeEntity entity) {
    return dao.batch(conn,
            c -> c.createStatement("INSERT INTO some_entity (svalue) VALUES ($1)").returnGeneratedValues("id"),
            Collections.singletonList(entity),
            (stmt, e) -> stmt.bind("$1", e.getSvalue()),
            (row, meta) -> row.get("id", Long.class)
        )
        .next()
        .map(id -> {
          entity.setId(id);
          return entity;
        });
  }

```

## Maven
Both milestone and snapshot artifacts (library, source, and javadoc) can be found in Maven repositories.

```xml
<dependency>
  <groupId>io.r2dbc</groupId>
  <artifactId>r2dbc-dao</artifactId>
  <version>1.0.0-RELEASE</version>
</dependency>
```


## License
This project is released under version 2.0 of the [Apache License][l].

[l]: https://www.apache.org/licenses/LICENSE-2.0
