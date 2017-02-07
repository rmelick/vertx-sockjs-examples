package com.github.rmelick;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.ext.web.handler.sockjs.SockJSHandlerOptions;
import io.vertx.ext.web.handler.sockjs.SockJSSocket;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;
import org.springframework.web.socket.handler.LoggingWebSocketHandlerDecorator;
import org.springframework.web.socket.sockjs.client.SockJsClient;
import org.springframework.web.socket.sockjs.client.Transport;
import org.springframework.web.socket.sockjs.client.WebSocketTransport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test sending requests in multiple frames from the client
 */
public class SockJSClientSendTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(SockJSClientSendTest.class);

	@Test
	public void testSimpleRequest() throws Exception {
		String vertxHost = "localhost";
		int vertxPort = 8080;

		// set up server
		Vertx vertx = setupVertxSockjsServer(vertxHost, vertxPort, new EchoBackSocketHandler());

		// set up client
		int expectedMessages = 1;
		CountDownLatch messageCountDown = new CountDownLatch(expectedMessages);
		AtomicLong receivedMessagesCounter = new AtomicLong(0);
		WebSocketSession session = setupSpringSockjsClient(vertxHost, vertxPort, messageCountDown, receivedMessagesCounter);

		session.sendMessage(new TextMessage("test"));
		boolean allMessagesReceived = messageCountDown.await(10, TimeUnit.SECONDS);
		assertTrue("Did not receive expected messages within 10 seconds", allMessagesReceived);
		assertEquals("Wrong number of received messages", expectedMessages, receivedMessagesCounter.get());

		vertx.close();
	}

	@Test
	public void testLargeRequest() throws Exception {
		String vertxHost = "localhost";
		int vertxPort = 8080;

		// set up server
		Vertx vertx = setupVertxSockjsServer(vertxHost, vertxPort, new FixedReplySocketHandler(Buffer.buffer("FIXED_REPLY")));

		// set up client
		int expectedMessages = 1;
		CountDownLatch messageCountDown = new CountDownLatch(expectedMessages);
		AtomicLong receivedMessagesCounter = new AtomicLong(0);
		WebSocketSession session = setupSpringSockjsClient(vertxHost, vertxPort, messageCountDown, receivedMessagesCounter);

		int approximateMessageKilobytes = 1;
		session.sendMessage(new TextMessage(getLargeMessage(approximateMessageKilobytes)));
		boolean allMessagesReceived = messageCountDown.await(10, TimeUnit.SECONDS);
		assertTrue("Did not receive expected messages within 10 seconds", allMessagesReceived);
		assertEquals("Wrong number of received messages", expectedMessages, receivedMessagesCounter.get());

		vertx.close();
	}

	private String getLargeMessage(int approximateMessageKilobytes) {
		String oneKbData = "DATA_START" + StringUtils.repeat(".", 1024 - "DATA_STARTDATA_END".length()) + "DATA_END";
		StringBuilder message = new StringBuilder(approximateMessageKilobytes);
		message.append("MESSAGE_START");
		for (int count = 0; count < approximateMessageKilobytes; count++) {
			message.append(oneKbData);
		}
		message.append("MESSAGE_END");
		return message.toString();
	}

	private WebSocketSession setupSpringSockjsClient(String vertxHost, int vertxPort, CountDownLatch messageCountDown,
			AtomicLong messageCounter)
			throws InterruptedException, java.util.concurrent.ExecutionException, java.util.concurrent.TimeoutException
	{
		List<Transport> transports = new ArrayList<>();
		transports.add(new WebSocketTransport(new StandardWebSocketClient()));
		SockJsClient sockJsClient = new SockJsClient(transports);
		return sockJsClient
				.doHandshake(new LoggingWebSocketHandlerDecorator(new CountingSocketHandler(messageCountDown, messageCounter)),
						String.format("ws://%s:%s/myapp", vertxHost, vertxPort))
				.get(10, TimeUnit.SECONDS);
	}

	private Vertx setupVertxSockjsServer(String vertxHost, int vertxPort, Handler<SockJSSocket> vertxSocketHandler) {
		LOGGER.info("Starting vertx");
		//set up server
		Vertx vertx = Vertx.vertx();

		Router router = Router.router(vertx);
		SockJSHandlerOptions options = new SockJSHandlerOptions().setHeartbeatInterval(2000);
		SockJSHandler sockJSHandler = SockJSHandler.create(vertx, options);
		sockJSHandler.socketHandler(vertxSocketHandler);
		router.route("/myapp/*").handler(sockJSHandler);
		HttpServerOptions httpServerOptions = new HttpServerOptions();
		httpServerOptions.setLogActivity(true);
		HttpServer server = vertx.createHttpServer(httpServerOptions);
		server.requestHandler(router::accept).listen(vertxPort, vertxHost);

		return vertx;
	}

	private static class CountingSocketHandler extends AbstractWebSocketHandler {
		private final CountDownLatch messageCountDown;
		private final AtomicLong messageCounter;

		public CountingSocketHandler(CountDownLatch messageCountDown, AtomicLong messageCounter) {
			this.messageCountDown = messageCountDown;
			this.messageCounter = messageCounter;
		}

		@Override
		public void handleMessage(WebSocketSession session, WebSocketMessage<?> message) throws Exception {
			messageCountDown.countDown();
			messageCounter.incrementAndGet();
			super.handleMessage(session, message);
		}
	}

	private static class EchoBackSocketHandler implements Handler<SockJSSocket> {
		@Override
		public void handle(SockJSSocket openedSocket) {
			openedSocket.handler(buffer -> {
				LOGGER.info("Server received buffer of size {}", buffer.length());
				LOGGER.info("Server replying with buffer of size {}", buffer.length());
				openedSocket.write(buffer);
				LOGGER.info("Server successfully replied with buffer of size {}", buffer.length());
			});
		}
	}

	private static class FixedReplySocketHandler implements Handler<SockJSSocket> {
		private final Buffer reply;

		public FixedReplySocketHandler(Buffer reply) {
			this.reply = reply;
		}

		@Override
		public void handle(SockJSSocket openedSocket) {
			openedSocket.handler(buffer -> {
				LOGGER.info("Server received buffer of size {}", buffer.length());
				LOGGER.info("Server replying with buffer of size {}", reply.length());
				openedSocket.write(reply);
				LOGGER.info("Server successfully replied with buffer of size {}", reply.length());
			});
		}
	}
}
