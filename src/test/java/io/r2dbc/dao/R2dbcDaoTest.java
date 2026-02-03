package io.r2dbc.dao;

import io.r2dbc.h2.H2ConnectionFactoryProvider;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.IsolationLevel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.r2dbc.h2.H2ConnectionFactoryProvider.URL;
import static io.r2dbc.spi.ConnectionFactoryOptions.*;

class R2dbcDaoTest {

  @RegisterExtension
  static final H2ServerExtension SERVER = new H2ServerExtension();

  private R2dbcDao dao;

  @BeforeEach
  void setUp() throws SQLException {
    this.dao = new R2dbcDao(ConnectionFactories.get(ConnectionFactoryOptions.builder()
        .option(DRIVER, H2ConnectionFactoryProvider.H2_DRIVER)
        .option(PASSWORD, "")
        .option(URL, SERVER.getR2dbcUrl())
        .option(USER, "sa")
        .build()));

    try (Connection conn = DriverManager.getConnection(SERVER.getJdbcUrl(), "sa", "");
         Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS test");
      stmt.execute("CREATE TABLE test ( id IDENTITY PRIMARY KEY, val INTEGER, name VARCHAR(50) )");
    }
  }

  @Test
  void executeInsert_IndexBinding() {
    dao.execute("INSERT INTO test (val, name) VALUES ($1, $2)", 100, "foo")
        .as(StepVerifier::create)
        .expectNext(1L)
        .verifyComplete();
  }

  @Test
  void executeInsert_NamedBinding() {
    dao.execute("INSERT INTO test (val, name) VALUES ($1, $2)", Map.of("$1", 200, "$2", "bar"))
        .as(StepVerifier::create)
        .expectNext(1L)
        .verifyComplete();
  }

  @Test
  void select_NullHandling() {
    // Insert NULL via JDBC helper
    try (Connection conn = DriverManager.getConnection(SERVER.getJdbcUrl(), "sa", "");
         Statement stmt = conn.createStatement()) {
      stmt.execute("INSERT INTO test (val, name) VALUES (300, NULL)");
    } catch (SQLException e) { throw new RuntimeException(e); }

    dao.select("SELECT name FROM test WHERE val = $1",
            (row, meta) -> Optional.ofNullable(row.get("name", String.class)),
            300)
        .as(StepVerifier::create)
        .expectNextMatches(Optional::isEmpty)
        .verifyComplete();
  }

  @Test
  void select_NamedBinding() {
    dao.execute("INSERT INTO test (val, name) VALUES (400, 'baz')").blockLast();

    dao.select("SELECT name FROM test WHERE val = $1",
            (row, meta) -> row.get("name", String.class),
            Map.of("$1", 400))
        .as(StepVerifier::create)
        .expectNext("baz")
        .verifyComplete();
  }

  @Test
  void batchExecution() {
    dao.batch("INSERT INTO test (val) VALUES ($1)",
            Arrays.asList(100, 200),
            (stmt, val) -> stmt.bind("$1", val))
        .as(StepVerifier::create)
        .expectNext(1L)
        .expectNext(1L)
        .verifyComplete();
  }

  @Test
  void batch_ReturnGeneratedValues() {
    dao.batch(
            c -> c.createStatement("INSERT INTO test (val) VALUES ($1)").returnGeneratedValues("id"),
            List.of(500, 600),
            (stmt, val) -> stmt.bind("$1", val),
            (row, meta) -> row.get("id", Long.class)
        )
        .as(StepVerifier::create)
        .expectNextCount(2)
        .verifyComplete();
  }

  @Test
  void select_ExistingConnection() {
    dao.withConnection(conn ->
        dao.execute(conn, "INSERT INTO test (val, name) VALUES (700, 'existing')")
            .thenMany(dao.select(conn, "SELECT name FROM test WHERE val = $1",
                (row, meta) -> row.get("name", String.class),
                700))
    )
    .as(StepVerifier::create)
    .expectNext("existing")
    .verifyComplete();
  }

  @Test
  void batch_ExistingConnection() {
    dao.withConnection(conn ->
        dao.batch(conn, "INSERT INTO test (val) VALUES ($1)",
            List.of(800, 900),
            (stmt, val) -> stmt.bind("$1", val))
    )
    .as(StepVerifier::create)
    .expectNext(1L)
    .expectNext(1L)
    .verifyComplete();
  }

  @Test
  void execute_ExistingConnection_NamedBinding() {
    dao.withConnection(conn ->
        dao.execute(conn, "INSERT INTO test (val, name) VALUES ($1, $2)", Map.of("$1", 1100, "$2", "named_exec"))
    )
    .as(StepVerifier::create)
    .expectNext(1L)
    .verifyComplete();
  }

  @Test
  void select_ExistingConnection_NamedBinding() {
    dao.execute("INSERT INTO test (val, name) VALUES (1200, 'named_select')").blockLast();

    dao.withConnection(conn ->
        dao.select(conn, "SELECT name FROM test WHERE val = $1",
            (row, meta) -> row.get("name", String.class),
            Map.of("$1", 1200))
    )
    .as(StepVerifier::create)
    .expectNext("named_select")
    .verifyComplete();
  }

  @Test
  void batch_ExistingConnection_ReturnGeneratedValues() {
    dao.withConnection(conn ->
        dao.batch(conn,
            c -> c.createStatement("INSERT INTO test (val) VALUES ($1)").returnGeneratedValues("id"),
            List.of(1300, 1400),
            (stmt, val) -> stmt.bind("$1", val),
            (row, meta) -> row.get("id", Long.class)
        )
    )
    .as(StepVerifier::create)
    .expectNextCount(2)
    .verifyComplete();
  }

  // -----------------------------------------------------------------------
  // Transaction Tests
  // -----------------------------------------------------------------------

  @Test
  void transactionCommit() {
    // We reuse the 'conn' provided by inTransaction to ensure both inserts happen in the same scope
    dao.inTransaction(conn ->
            Flux.concat(
                dao.execute(conn, "INSERT INTO test (val) VALUES (100)"),
                dao.execute(conn, "INSERT INTO test (val) VALUES (200)")
            )
        )
        .as(StepVerifier::create)
        .expectNext(1L) // Result of first insert
        .expectNext(1L) // Result of second insert
        .verifyComplete();

    // Verify both were committed
    dao.select("SELECT count(*) FROM test", (row, meta) -> row.get(0, Long.class))
        .as(StepVerifier::create)
        .expectNext(2L)
        .verifyComplete();
  }

  @Test
  void transactionRollback() {
    dao.inTransaction(conn ->
            Flux.concat(
                dao.execute(conn, "INSERT INTO test (val) VALUES (100)"),
                Mono.error(new RuntimeException("Boom")) // Fail halfway
            )
        )
        .as(StepVerifier::create)
        .expectNextCount(1) // The first insert might emit before the error happens
        .verifyError(RuntimeException.class);

    // Verify Rollback: Table should be empty
    dao.select("SELECT count(*) FROM test", (row, meta) -> row.get(0, Long.class))
        .as(StepVerifier::create)
        .expectNext(0L)
        .verifyComplete();
  }

  @Test
  void transaction_IsolationLevel() {
    dao.inTransaction(IsolationLevel.READ_COMMITTED, conn ->
            dao.execute(conn, "INSERT INTO test (val) VALUES (1000)")
        )
        .as(StepVerifier::create)
        .expectNext(1L)
        .verifyComplete();

    dao.select("SELECT val FROM test WHERE val = 1000", (row, meta) -> row.get("val", Integer.class))
        .as(StepVerifier::create)
        .expectNext(1000)
        .verifyComplete();
  }
}