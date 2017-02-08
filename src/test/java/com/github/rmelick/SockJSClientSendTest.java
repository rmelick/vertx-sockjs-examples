package com.github.rmelick;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Splitter;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.ext.web.handler.sockjs.SockJSHandlerOptions;
import io.vertx.ext.web.handler.sockjs.SockJSSocket;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.AbstractWebSocketHandler;
import org.springframework.web.socket.handler.LoggingWebSocketHandlerDecorator;
import org.springframework.web.socket.sockjs.client.SockJsClient;
import org.springframework.web.socket.sockjs.client.Transport;
import org.springframework.web.socket.sockjs.client.WebSocketTransport;

import javax.websocket.ContainerProvider;
import javax.websocket.WebSocketContainer;

import static org.junit.Assert.assertEquals;

/**
 * Test sending requests in multiple frames from the client
 */
public class SockJSClientSendTest {
	private static final Logger LOGGER = LoggerFactory.getLogger(SockJSClientSendTest.class);
	public static final int TEST_WAIT_TIME_MILLIS = 1_000;

	/**
	 * This test demonstrates a simple test, to make sure that the spring client and vertx server can communicate
	 */
	@Test
	public void testSimpleRequest() throws Exception {
		String vertxHost = "localhost";
		int vertxPort = 8080;

		// set up server
		HttpServer httpServer = setupVertxSockjsServer(vertxHost, vertxPort, new EchoBackSocketHandler());

		// set up client
		int expectedMessages = 1;
		AtomicLong receivedMessagesCounter = new AtomicLong(0);
		AtomicReference<String> error = new AtomicReference<>();
		WebSocketSession session = setupSpringSockjsClient(vertxHost, vertxPort, receivedMessagesCounter, error);

		session.sendMessage(new TextMessage("test"));
		Thread.sleep(TEST_WAIT_TIME_MILLIS);
		assertEquals("Should not have received any errors", null, error.get());
		assertEquals("Wrong number of received messages", expectedMessages, receivedMessagesCounter.get());

		httpServer.close();
	}

	/**
	 * This test shows requests that fail because the they are too large for the tomcat websocket to handle.
	 * It shows that clients need to break up their messages into frames
	 */
	@Test
	public void testLargeRequestSingleFrame() throws Exception {
		String vertxHost = "localhost";
		int vertxPort = 8081;

		// set up server
		HttpServer httpServer = setupVertxSockjsServer(vertxHost, vertxPort, new FixedReplySocketHandler(Buffer.buffer("FIXED_REPLY")));

		// set up client
		int expectedMessages = 1;
		AtomicLong receivedMessagesCounter = new AtomicLong(0);
		AtomicReference<String> error = new AtomicReference<>();
		WebSocketSession session = setupSpringSockjsClient(vertxHost, vertxPort, receivedMessagesCounter, error);

		int approximateMessageKilobytes = 10_000;
		session.sendMessage(new TextMessage(getLargeMessage(approximateMessageKilobytes)));

		Thread.sleep(TEST_WAIT_TIME_MILLIS);
		assertEquals("Received error", null, error.get());
		assertEquals("Wrong number of received messages", expectedMessages, receivedMessagesCounter.get());

		httpServer.close();
	}

	/**
	 * This test should succeed because the client has broken the large message up into smaller frames/pieces/chunks.
	 * The client should only receive a single reply from the server though, as the server is supposed to recombine
	 * the pieces into a single message.
	 */
	@Test
	public void testLargeRequestMultipleFrame() throws Exception {
		String vertxHost = "localhost";
		int vertxPort = 8082;

		// set up server
		HttpServer httpServer = setupVertxSockjsServer(vertxHost, vertxPort, new FixedReplySocketHandler(Buffer.buffer("FIXED_REPLY")));


		int approximateMessageKilobytes = 10;
		List<TextMessage> multipleFrames = splitIntoMessages(getLargeMessage(approximateMessageKilobytes));

		// set up client
		int expectedMessages = 1;
		AtomicLong receivedMessagesCounter = new AtomicLong(0);
		AtomicReference<String> error = new AtomicReference<>();
		WebSocketSession session = setupSpringSockjsClient(vertxHost, vertxPort, receivedMessagesCounter, error);

		for (TextMessage frame : multipleFrames) {
			session.sendMessage(frame);
		}

		Thread.sleep(TEST_WAIT_TIME_MILLIS);
		assertEquals("Should not have received any errors", null, error.get());
		assertEquals("Wrong number of received messages", expectedMessages, receivedMessagesCounter.get());

		httpServer.close();
	}

	/**
	 * The vertx server is able to send multiple frames to the client, and the client is able to recombine them
	 * into a single message.
	 */
	@Test
	public void testLargeResponse() throws Exception {
		String vertxHost = "localhost";
		int vertxPort = 8083;

		// set up server
		int approximateMessageKilobytes = 1_000;
		Buffer largeReply = Buffer.buffer(getLargeMessage(approximateMessageKilobytes));
		HttpServer httpServer = setupVertxSockjsServer(vertxHost, vertxPort, new FixedReplySocketHandler(largeReply));

		// set up client
		int expectedMessages = 1;
		AtomicLong receivedMessagesCounter = new AtomicLong(0);
		AtomicReference<String> error = new AtomicReference<>();
		WebSocketSession session = setupSpringSockjsClient(vertxHost, vertxPort, receivedMessagesCounter, error);

		session.sendMessage(new TextMessage("test"));

		Thread.sleep(TEST_WAIT_TIME_MILLIS);
		assertEquals("Should not have received any errors", null, error.get());
		assertEquals("Wrong number of received messages", expectedMessages, receivedMessagesCounter.get());
		httpServer.close();
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

	private WebSocketSession setupSpringSockjsClient(String vertxHost, int vertxPort,	AtomicLong messageCounter,
																									 AtomicReference<String> error) throws Exception {
		List<Transport> transports = new ArrayList<>();
		int maxBufferBytes = 10_000_000; // 10 mb messages
		WebSocketContainer webSocketContainer = ContainerProvider.getWebSocketContainer();
		webSocketContainer.setDefaultMaxBinaryMessageBufferSize(maxBufferBytes);
		webSocketContainer.setDefaultMaxTextMessageBufferSize(maxBufferBytes);
		StandardWebSocketClient webSocketClient = new StandardWebSocketClient(webSocketContainer);
		transports.add(new WebSocketTransport(webSocketClient));
		SockJsClient sockJsClient = new SockJsClient(transports);
		WebSocketHandler handler = new LoggingWebSocketHandlerDecorator(
				new CountingSocketHandler(messageCounter, error));
		return sockJsClient
				.doHandshake(handler, String.format("ws://%s:%s/myapp", vertxHost, vertxPort))
				.get(10, TimeUnit.SECONDS);
	}

	private HttpServer setupVertxSockjsServer(String vertxHost, int vertxPort, Handler<SockJSSocket> vertxSocketHandler) {
		LOGGER.info("Starting vertx");
		//set up server
		Vertx vertx = Vertx.vertx();

		Router router = Router.router(vertx);
		SockJSHandlerOptions options = new SockJSHandlerOptions().setHeartbeatInterval(2000);
		SockJSHandler sockJSHandler = SockJSHandler.create(vertx, options);
		sockJSHandler.socketHandler(vertxSocketHandler);
		// TODO can we put wrap the SockJSHandler inside a custom handler that would support re-combining
		// messages from incomplete frames before passing it to the SockJSHandler.  This way the sockjs
		// code would see it as a single frame.
		router.route("/myapp/*").handler(sockJSHandler);
		HttpServerOptions httpServerOptions = new HttpServerOptions();
		httpServerOptions.setLogActivity(true);
		HttpServer server = vertx.createHttpServer(httpServerOptions);
		server.requestHandler(router::accept).listen(vertxPort, vertxHost);

		return server;
	}

	private static class CountingSocketHandler extends AbstractWebSocketHandler {
		private final AtomicLong messageCounter;
		private final AtomicReference<String> error;

		public CountingSocketHandler(AtomicLong messageCounter,	AtomicReference<String> error) {
			this.messageCounter = messageCounter;
			this.error = error;
		}

		@Override
		public void handleMessage(WebSocketSession session, WebSocketMessage<?> message) throws Exception {
			try {
				messageCounter.incrementAndGet();
				super.handleMessage(session, message);
			} catch (Throwable t) {
				error.set("Exception while processing message " + t.getMessage());
				LOGGER.error(error.get(), t);
			}
		}

		@Override
		public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
			error.set("Transport error " + exception.getMessage());
			LOGGER.error(error.get(), exception);
			super.handleTransportError(session, exception);
		}

		@Override
		public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
			if (!CloseStatus.NORMAL.equalsCode(status)) {
				error.set("Connection closed with non-normal status " + status);
				LOGGER.error(error.get());
			}
			super.afterConnectionClosed(session, status);
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
