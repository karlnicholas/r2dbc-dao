package io.r2dbc.dao;

import org.h2.tools.Server;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

public final class H2ServerExtension implements BeforeAllCallback, AfterAllCallback {

  private Server server;
  private int port;

  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception {
    this.server = Server.createTcpServer("-tcpPort", "0", "-tcpAllowOthers", "-ifNotExists").start();
    this.port = this.server.getPort();
  }

  @Override
  public void afterAll(ExtensionContext extensionContext) {
    if (this.server != null) {
      this.server.stop();
    }
  }

  public String getJdbcUrl() {
    return String.format("jdbc:h2:tcp://localhost:%d/mem:testdb;DB_CLOSE_DELAY=-1", this.port);
  }

  public String getR2dbcUrl() {
    return String.format("tcp://localhost:%d/mem:testdb;DB_CLOSE_DELAY=-1", this.port);
  }
}