package io.r2dbc.dao;

import io.r2dbc.spi.*;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A minimalist R2DBC Data Access Object.
 * Updates: Added 'IsolationLevel' overload for inTransaction.
 */
public class R2dbcDao {

  private final ConnectionFactory connectionFactory;

  public R2dbcDao(ConnectionFactory connectionFactory) {
    if (connectionFactory == null) throw new IllegalArgumentException("ConnectionFactory must not be null");
    this.connectionFactory = connectionFactory;
  }

  // -------------------------------------------------------------------------
  // 1. Core Lifecycle Management
  // -------------------------------------------------------------------------

  public <T> Flux<T> withConnection(Function<Connection, ? extends Publisher<T>> action) {
    return Flux.usingWhen(
        connectionFactory.create(),
        connection -> Flux.from(action.apply(connection)),
        Connection::close
    );
  }

  // Standard Transaction
  public <T> Flux<T> inTransaction(Function<Connection, ? extends Publisher<T>> action) {
    return inTransaction(null, action);
  }

  // FIX: Added Overload for IsolationLevel
  public <T> Flux<T> inTransaction(@Nullable IsolationLevel isolationLevel,
                                   Function<Connection, ? extends Publisher<T>> action) {
    return withConnection(conn -> {
      // 1. Setup Isolation (if requested)
      Mono<Void> setup = (isolationLevel != null)
          ? Mono.from(conn.setTransactionIsolationLevel(isolationLevel))
          : Mono.empty();

      // 2. Begin -> Action -> Commit (or Rollback on error)
      return setup.then(Mono.from(conn.beginTransaction()))
          .thenMany(Flux.from(action.apply(conn)))
          .concatWith(Flux.defer(() -> Mono.from(conn.commitTransaction()).then(Mono.empty())))
          .onErrorResume(e -> Mono.from(conn.rollbackTransaction()).then(Mono.error(e)));
    });
  }

  // -------------------------------------------------------------------------
  // 2. Convenience Methods (Auto-manage Connection)
  // -------------------------------------------------------------------------

  public Flux<Long> execute(String sql, Object... parameters) {
    return withConnection(conn -> execute(conn, sql, parameters));
  }

  public <T> Flux<T> select(String sql, BiFunction<Row, RowMetadata, T> mapper, Object... parameters) {
    return withConnection(conn -> select(conn, sql, mapper, parameters));
  }

  public Flux<Long> execute(String sql, Map<String, Object> parameters) {
    return withConnection(conn -> execute(conn, sql, parameters));
  }

  public <T> Flux<T> select(String sql, BiFunction<Row, RowMetadata, T> mapper, Map<String, Object> parameters) {
    return withConnection(conn -> select(conn, sql, mapper, parameters));
  }

  public <T> Flux<Long> batch(String sql, Iterable<T> items, BiConsumer<Statement, T> binder) {
    return withConnection(conn -> batch(conn, sql, items, binder));
  }

  public <T, R> Flux<R> batch(Function<Connection, Statement> statementFactory,
                              Iterable<T> items,
                              BiConsumer<Statement, T> binder,
                              BiFunction<Row, RowMetadata, R> mapper) {
    return withConnection(conn -> batch(conn, statementFactory, items, binder, mapper));
  }

  // -------------------------------------------------------------------------
  // 3. Composable Operations (Take existing Connection)
  // -------------------------------------------------------------------------

  public Flux<Long> execute(Connection conn, String sql, Object... parameters) {
    Statement statement = conn.createStatement(sql);
    bindParameters(statement, parameters);
    return Flux.from(statement.execute()).concatMap(Result::getRowsUpdated);
  }

  public <T> Flux<T> select(Connection conn, String sql, BiFunction<Row, RowMetadata, T> mapper, Object... parameters) {
    Statement statement = conn.createStatement(sql);
    bindParameters(statement, parameters);
    return Flux.from(statement.execute()).concatMap(result -> result.map(mapper));
  }

  public Flux<Long> execute(Connection conn, String sql, Map<String, Object> parameters) {
    Statement statement = conn.createStatement(sql);
    bindNamedParameters(statement, parameters);
    return Flux.from(statement.execute()).concatMap(Result::getRowsUpdated);
  }

  public <T> Flux<T> select(Connection conn, String sql, BiFunction<Row, RowMetadata, T> mapper, Map<String, Object> parameters) {
    Statement statement = conn.createStatement(sql);
    bindNamedParameters(statement, parameters);
    return Flux.from(statement.execute()).concatMap(result -> result.map(mapper));
  }

  public <T> Flux<Long> batch(Connection conn, String sql, Iterable<T> items, BiConsumer<Statement, T> binder) {
    return Flux.from(prepareBatch(conn, c -> c.createStatement(sql), items, binder).execute())
        .concatMap(Result::getRowsUpdated);
  }

  public <T, R> Flux<R> batch(Connection conn,
                              Function<Connection, Statement> statementFactory,
                              Iterable<T> items,
                              BiConsumer<Statement, T> binder,
                              BiFunction<Row, RowMetadata, R> mapper) {
    return Flux.from(prepareBatch(conn, statementFactory, items, binder).execute())
        .concatMap(result -> result.map(mapper));
  }

  // -------------------------------------------------------------------------
  // Internal Helpers
  // -------------------------------------------------------------------------

  private <T> Statement prepareBatch(Connection conn,
                                     Function<Connection, Statement> factory,
                                     Iterable<T> items,
                                     BiConsumer<Statement, T> binder) {
    Statement statement = factory.apply(conn);
    Iterator<T> iterator = items.iterator();
    boolean first = true;
    while (iterator.hasNext()) {
      T item = iterator.next();
      if (!first) statement.add();
      binder.accept(statement, item);
      first = false;
    }
    return statement;
  }

  private void bindParameters(Statement statement, @Nullable Object[] parameters) {
    if (parameters == null) return;
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
    if (parameters == null) return;
    parameters.forEach((name, value) -> {
      if (value != null) {
        statement.bind(name, value);
      } else {
        statement.bindNull(name, String.class);
      }
    });
  }
}