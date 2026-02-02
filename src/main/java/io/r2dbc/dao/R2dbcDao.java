package io.r2dbc.dao;

import io.r2dbc.spi.*;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A minimalist R2DBC Data Access Object that provides a functional wrapper around the
 * R2DBC SPI. It manages connection lifecycles, transactions, and statement binding.
 * <p>
 * This class uses SLF4J for neutral, facade-based logging.
 */
public class R2dbcDao {

  private static final Logger log = LoggerFactory.getLogger(R2dbcDao.class);
  private final ConnectionFactory connectionFactory;

  /**
   * Creates a new DAO instance.
   *
   * @param connectionFactory the R2DBC {@link ConnectionFactory} to use for obtaining connections.
   * @throws IllegalArgumentException if connectionFactory is null.
   */
  public R2dbcDao(final ConnectionFactory connectionFactory) {
    if (connectionFactory == null) throw new IllegalArgumentException("ConnectionFactory must not be null");
    this.connectionFactory = connectionFactory;
  }

  // -------------------------------------------------------------------------
  // 1. Core Lifecycle Management
  // -------------------------------------------------------------------------

  /**
   * Executes a function within the scope of a managed connection.
   * The connection is created before the action and closed after the action completes,
   * regardless of success or failure.
   *
   * @param action the function to execute with the connection.
   * @param <T>    the type of element emitted by the returned Flux.
   * @return a Flux emitting the results of the action.
   */
  public <T> Flux<T> withConnection(final Function<Connection, ? extends Publisher<T>> action) {
    return Flux.usingWhen(
        connectionFactory.create(),
        connection -> {
          if (log.isDebugEnabled()) {
            log.debug("Connection acquired from factory");
          }
          return Flux.from(action.apply(connection));
        },
        connection -> {
          if (log.isDebugEnabled()) {
            log.debug("Closing connection");
          }
          return connection.close();
        }
    );
  }

  /**
   * Executes a function within a managed transaction using the default isolation level.
   *
   * @param action the function to execute within the transaction.
   * @param <T>    the type of element emitted by the returned Flux.
   * @return a Flux emitting the results of the transactional action.
   * @see #inTransaction(IsolationLevel, Function)
   */
  public <T> Flux<T> inTransaction(final Function<Connection, ? extends Publisher<T>> action) {
    return inTransaction(null, action);
  }

  /**
   * Executes a function within a managed transaction with a specific {@link IsolationLevel}.
   * <p>
   * The transaction is committed if the Flux completes successfully.
   * If an error occurs, the transaction is rolled back.
   *
   * @param isolationLevel the isolation level to use, or null for the default.
   * @param action         the function to execute within the transaction.
   * @param <T>            the type of element emitted by the returned Flux.
   * @return a Flux emitting the results of the transactional action.
   */
  public <T> Flux<T> inTransaction(@Nullable final IsolationLevel isolationLevel,
                                   final Function<Connection, ? extends Publisher<T>> action) {
    return withConnection(conn -> {
      // 1. Setup Isolation (if requested)
      Mono<Void> setup = (isolationLevel != null)
          ? Mono.from(conn.setTransactionIsolationLevel(isolationLevel))
          .doOnSuccess(v -> log.debug("Set transaction isolation level: {}", isolationLevel))
          : Mono.empty();

      // 2. Begin -> Action -> Commit (or Rollback on error)
      return setup.then(Mono.from(conn.beginTransaction()))
          .doOnSuccess(v -> log.debug("Began transaction"))
          .thenMany(Flux.from(action.apply(conn)))
          .concatWith(Flux.defer(() -> Mono.from(conn.commitTransaction())
              .doOnSuccess(v -> log.debug("Committed transaction"))
              .then(Mono.empty())))
          .onErrorResume(e -> {
            log.error("Error during transaction, rolling back.", e);
            return Mono.from(conn.rollbackTransaction()).then(Mono.error(e));
          });
    });
  }

  // -------------------------------------------------------------------------
  // 2. Convenience Methods (Auto-manage Connection)
  // -------------------------------------------------------------------------

  /**
   * Executes a SQL statement (UPDATE, INSERT, DELETE) and returns the number of rows updated.
   * Manages the connection automatically.
   *
   * @param sql        the SQL statement to execute.
   * @param parameters optional indexed parameters to bind.
   * @return a Flux emitting the count of rows updated.
   */
  public Flux<Long> execute(final String sql, final Object... parameters) {
    return withConnection(conn -> execute(conn, sql, parameters));
  }

  /**
   * Executes a SQL SELECT statement and maps the results using the provided mapper.
   * Manages the connection automatically.
   *
   * @param sql        the SQL statement to execute.
   * @param mapper     the function to map Row and RowMetadata to the desired type.
   * @param parameters optional indexed parameters to bind.
   * @param <T>        the return type.
   * @return a Flux emitting mapped entities.
   */
  public <T> Flux<T> select(final String sql, final BiFunction<Row, RowMetadata, T> mapper, Object... parameters) {
    return withConnection(conn -> select(conn, sql, mapper, parameters));
  }

  /**
   * Executes a SQL statement (UPDATE, INSERT, DELETE) using named parameters.
   * Manages the connection automatically.
   *
   * @param sql        the SQL statement to execute.
   * @param parameters a map of named parameters to bind.
   * @return a Flux emitting the count of rows updated.
   */
  public Flux<Long> execute(final String sql, final Map<String, Object> parameters) {
    return withConnection(conn -> execute(conn, sql, parameters));
  }

  /**
   * Executes a SQL SELECT statement using named parameters.
   * Manages the connection automatically.
   *
   * @param sql        the SQL statement to execute.
   * @param mapper     the function to map Row and RowMetadata to the desired type.
   * @param parameters a map of named parameters to bind.
   * @param <T>        the return type.
   * @return a Flux emitting mapped entities.
   */
  public <T> Flux<T> select(final String sql, final BiFunction<Row, RowMetadata, T> mapper, Map<String, Object> parameters) {
    return withConnection(conn -> select(conn, sql, mapper, parameters));
  }

  /**
   * Executes a batch update/insert operation.
   * Manages the connection automatically.
   *
   * @param sql    the SQL statement template.
   * @param items  the iterable of items to process in the batch.
   * @param binder the consumer to bind parameters from the item to the statement.
   * @param <T>    the type of the item being batched.
   * @return a Flux emitting the rows updated for each statement in the batch.
   */
  public <T> Flux<Long> batch(final String sql, final Iterable<T> items, BiConsumer<Statement, T> binder) {
    return withConnection(conn -> batch(conn, sql, items, binder));
  }

  /**
   * Executes a batch operation that returns generated keys (or other results).
   * Manages the connection automatically.
   *
   * @param statementFactory factory to create the statement (e.g. enabling returnGeneratedValues).
   * @param items            the iterable of items to process.
   * @param binder           the consumer to bind parameters.
   * @param mapper           the mapper for the result.
   * @param <T>              the type of the item.
   * @param <R>              the result type.
   * @return a Flux emitting the mapped results.
   */
  public <T, R> Flux<R> batch(final Function<Connection, Statement> statementFactory,
                              final Iterable<T> items,
                              final BiConsumer<Statement, T> binder,
                              final BiFunction<Row, RowMetadata, R> mapper) {
    return withConnection(conn -> batch(conn, statementFactory, items, binder, mapper));
  }

  // -------------------------------------------------------------------------
  // 3. Composable Operations (Take existing Connection)
  // -------------------------------------------------------------------------

  /**
   * Executes a SQL statement on an existing connection.
   *
   * @param conn       the existing connection.
   * @param sql        the SQL to execute.
   * @param parameters indexed parameters.
   * @return a Flux emitting rows updated.
   */
  public Flux<Long> execute(final Connection conn, final String sql, final Object... parameters) {
    if (log.isDebugEnabled()) {
      log.debug("Execute SQL: {}", sql);
    }
    Statement statement = conn.createStatement(sql);
    bindParameters(statement, parameters);
    return Flux.from(statement.execute()).concatMap(Result::getRowsUpdated);
  }

  /**
   * Executes a SQL SELECT on an existing connection.
   *
   * @param conn       the existing connection.
   * @param sql        the SQL to execute.
   * @param mapper     the result mapper.
   * @param parameters indexed parameters.
   * @param <T>        the return type.
   * @return a Flux of mapped results.
   */
  public <T> Flux<T> select(final Connection conn, final String sql, final BiFunction<Row, RowMetadata, T> mapper, Object... parameters) {
    if (log.isDebugEnabled()) {
      log.debug("Select SQL: {}", sql);
    }
    Statement statement = conn.createStatement(sql);
    bindParameters(statement, parameters);
    return Flux.from(statement.execute()).concatMap(result -> result.map(mapper));
  }

  /**
   * Executes a SQL statement on an existing connection using named parameters.
   *
   * @param conn       the existing connection.
   * @param sql        the SQL to execute.
   * @param parameters named parameters.
   * @return a Flux emitting rows updated.
   */
  public Flux<Long> execute(final Connection conn, final String sql, final Map<String, Object> parameters) {
    if (log.isDebugEnabled()) {
      log.debug("Execute SQL: {}", sql);
    }
    Statement statement = conn.createStatement(sql);
    bindNamedParameters(statement, parameters);
    return Flux.from(statement.execute()).concatMap(Result::getRowsUpdated);
  }

  /**
   * Executes a SQL SELECT on an existing connection using named parameters.
   *
   * @param conn       the existing connection.
   * @param sql        the SQL to execute.
   * @param mapper     the result mapper.
   * @param parameters named parameters.
   * @param <T>        the return type.
   * @return a Flux of mapped results.
   */
  public <T> Flux<T> select(final Connection conn, final String sql, final BiFunction<Row, RowMetadata, T> mapper, Map<String, Object> parameters) {
    if (log.isDebugEnabled()) {
      log.debug("Select SQL: {}", sql);
    }
    Statement statement = conn.createStatement(sql);
    bindNamedParameters(statement, parameters);
    return Flux.from(statement.execute()).concatMap(result -> result.map(mapper));
  }

  /**
   * Batches operations on an existing connection.
   *
   * @param conn   the existing connection.
   * @param sql    the SQL template.
   * @param items  items to batch.
   * @param binder binder for items.
   * @param <T>    item type.
   * @return a Flux of rows updated.
   */
  public <T> Flux<Long> batch(final Connection conn, final String sql, final Iterable<T> items, BiConsumer<Statement, T> binder) {
    if (log.isDebugEnabled()) {
      log.debug("Batch Execute SQL: {}", sql);
    }
    return Flux.from(prepareBatch(conn, c -> c.createStatement(sql), items, binder).execute())
        .concatMap(Result::getRowsUpdated);
  }

  /**
   * Batches operations on an existing connection with a custom statement factory.
   *
   * @param conn             the existing connection.
   * @param statementFactory factory for the statement.
   * @param items            items to batch.
   * @param binder           binder for items.
   * @param mapper           result mapper.
   * @param <T>              item type.
   * @param <R>              result type.
   * @return a Flux of mapped results.
   */
  public <T, R> Flux<R> batch(final Connection conn,
                              final Function<Connection, Statement> statementFactory,
                              final Iterable<T> items,
                              final BiConsumer<Statement, T> binder,
                              final BiFunction<Row, RowMetadata, R> mapper) {
    if (log.isDebugEnabled()) {
      log.debug("Batch Execute (Custom Factory)");
    }
    return Flux.from(prepareBatch(conn, statementFactory, items, binder).execute())
        .concatMap(result -> result.map(mapper));
  }

  // -------------------------------------------------------------------------
  // Internal Helpers
  // -------------------------------------------------------------------------

  private <T> Statement prepareBatch(final Connection conn,
                                     final Function<Connection, Statement> factory,
                                     final Iterable<T> items,
                                     final BiConsumer<Statement, T> binder) {
    Statement statement = factory.apply(conn);
    Iterator<T> iterator = items.iterator();
    boolean first = true;
    int count = 0;

    while (iterator.hasNext()) {
      T item = iterator.next();
      if (!first) statement.add();
      binder.accept(statement, item);
      first = false;
      count++;
    }

    if (log.isTraceEnabled()) {
      log.trace("Prepared batch with {} items", count);
    }

    return statement;
  }

  private void bindParameters(final Statement statement, @Nullable final Object[] parameters) {
    if (parameters == null || parameters.length == 0) return;

    if (log.isTraceEnabled()) {
      log.trace("Binding indexed parameters: count={}", parameters.length);
    }

    for (int i = 0; i < parameters.length; i++) {
      Object value = parameters[i];
      if (value != null) {
        statement.bind(i, value);
      } else {
        statement.bindNull(i, String.class);
      }
    }
  }

  private void bindNamedParameters(Statement statement, @Nullable Map<String, Object> parameters) {
    if (parameters == null || parameters.isEmpty()) return;

    if (log.isTraceEnabled()) {
      log.trace("Binding named parameters: {}", parameters.keySet());
    }

    parameters.forEach((name, value) -> {
      if (value != null) {
        statement.bind(name, value);
      } else {
        statement.bindNull(name, String.class);
      }
    });
  }
}