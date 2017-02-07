package com.github.rmelick;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Splitter;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test sending requests in multiple frames from the client
 */
public class SockJSClientSendTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(SockJSClientSendTest.class);

	/**
	 * This test demonstrates a simple test, to make sure that the spring client and vertx server can communicate
	 */
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

	/**
	 * This test shows requests that fail because the they are too large for the tomcat websocket to handle.
	 * It shows that clients need to break up their messages into frames
	 */
	@Test
	public void testLargeRequestSingleFrame() throws Exception {
		String vertxHost = "localhost";
		int vertxPort = 8080;

		// set up server
		Vertx vertx = setupVertxSockjsServer(vertxHost, vertxPort, new FixedReplySocketHandler(Buffer.buffer("FIXED_REPLY")));

		// set up client
		int expectedMessages = 0;
		CountDownLatch messageCountDown = new CountDownLatch(1);
		AtomicLong receivedMessagesCounter = new AtomicLong(0);
		WebSocketSession session = setupSpringSockjsClient(vertxHost, vertxPort, messageCountDown, receivedMessagesCounter);

		int approximateMessageKilobytes = 10;
		session.sendMessage(new TextMessage(getLargeMessage(approximateMessageKilobytes)));
		boolean allMessagesReceived = messageCountDown.await(10, TimeUnit.SECONDS);
		assertFalse("Should not have received any messages within 10 seconds", allMessagesReceived);
		assertEquals("Wrong number of received messages", expectedMessages, receivedMessagesCounter.get());

		vertx.close();
	}

	/**
	 * This test should succeed because the client has broken the large message up into smaller frames/pieces/chunks
	 */
	@Test
	public void testLargeRequestMultipleFrame() throws Exception {
		String vertxHost = "localhost";
		int vertxPort = 8080;

		// set up server
		Vertx vertx = setupVertxSockjsServer(vertxHost, vertxPort, new FixedReplySocketHandler(Buffer.buffer("FIXED_REPLY")));


		int approximateMessageKilobytes = 10;
		List<TextMessage> multipleFrames = splitIntoMessages(getLargeMessage(approximateMessageKilobytes));

		// set up client
		int expectedMessages = multipleFrames.size();
		CountDownLatch messageCountDown = new CountDownLatch(expectedMessages);
		AtomicLong receivedMessagesCounter = new AtomicLong(0);
		WebSocketSession session = setupSpringSockjsClient(vertxHost, vertxPort, messageCountDown, receivedMessagesCounter);

		for (TextMessage frame : multipleFrames) {
			session.sendMessage(frame);
		}
		boolean allMessagesReceived = messageCountDown.await(10, TimeUnit.SECONDS);
		assertTrue("Did not receive expected messages within 10 seconds", allMessagesReceived);
		assertEquals("Wrong number of received messages", expectedMessages, receivedMessagesCounter.get());

		vertx.close();
	}

	private List<TextMessage> splitIntoMessages(String fullText) {
		List<TextMessage> messages = new ArrayList<>();
		List<String> splitStrings = new ArrayList<>(Splitter.fixedLength(1024).splitToList(fullText));
		String last = splitStrings.remove(splitStrings.size() - 1);
		for (String individualMessage : splitStrings) {
			messages.add(new TextMessage(individualMessage, false));
		}
		messages.add(new TextMessage(last, true));
		return messages;
	}

	private String getLargeMessage(int approximateMessageKilobytes) {
		String oneKbData = "KB_START_" + StringUtils.repeat("d", 1024 - "KB_START__KB_END".length()) + "_KB_END";
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
