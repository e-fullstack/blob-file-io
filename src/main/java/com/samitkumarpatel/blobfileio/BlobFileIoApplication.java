package com.samitkumarpatel.blobfileio;

import com.azure.core.util.FluxUtil;
import com.azure.storage.blob.BlobAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.models.ParallelTransferOptions;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.http.codec.multipart.Part;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.server.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.springframework.http.MediaType.*;
import static org.springframework.web.reactive.function.server.RequestPredicates.*;
import static org.springframework.web.reactive.function.server.ServerResponse.ok;

@SpringBootApplication
public class BlobFileIoApplication {

	public static void main(String[] args) {
		SpringApplication.run(BlobFileIoApplication.class, args);
	}

}
@Configuration
class Router {
	@Bean
	public RouterFunction<ServerResponse> route(Handler handler) {
		return RouterFunctions.route()
				.path("/azure", pathBuilder -> pathBuilder
						.GET("/download",accept(APPLICATION_OCTET_STREAM) , handler::download)
						.POST("/upload", accept(MULTIPART_FORM_DATA), handler::upload)
				).build();
	}
}

@Configuration
@RequiredArgsConstructor
@Slf4j
class Handler {
	private final FileIO fileIO;
	public Mono<ServerResponse> download(ServerRequest request) {
		var blobIdentifier = request.queryParam("blobIdentifier").get();
		return ok()
				.header("Content-Disposition", "attachment; filename=%s".formatted(blobIdentifier))
				.body(Mono.just("Hello World".getBytes(StandardCharsets.UTF_8)), byte[].class);
		/*
		return fileIO
				.download(blobIdentifier)
				.flatMap(byteBuffer -> ok().body(byteBuffer, ByteBuffer.class));

		 */
	}

	public Mono<ServerResponse> upload(ServerRequest request) {
		return request.multipartData().flatMap(stringPartMultiValueMap -> {
			Map<String, Part> partMap = stringPartMultiValueMap.toSingleValueMap();
			var filePart = (FilePart) partMap.get("file");
			var fileName = filePart.filename();
			var fileContent = filePart.content().flatMap(dataBuffer -> FluxUtil.toFluxByteBuffer(dataBuffer.asInputStream()));
			return fileIO.upload(fileName, fileContent)
					.flatMap(uploadMessage -> ok().body(Mono.just(uploadMessage), String.class));
		});
	}
}

@Service
@RequiredArgsConstructor
@Slf4j
/**
 * azure cloud sdk
 * https://learn.microsoft.com/en-us/java/api/com.azure.storage.blob.blobserviceasyncclient?view=azure-java-stable
 */
class FileIO {
	@Value("${spring.cloud.azure.storage.container-name}")
	private String containerName;
	private final BlobServiceAsyncClient blobServiceAsyncClient;

	public Mono<String> upload(String blobIdentifier, Flux<ByteBuffer> data) {
		long blockSize = 2 * 1024 * 1024;
		return blobServiceAsyncClient.getBlobContainerAsyncClient(containerName)
				.getBlobAsyncClient(blobIdentifier)
				.upload(data, new ParallelTransferOptions().setBlockSizeLong(blockSize).setMaxConcurrency(2), true)
				.flatMap(blockBlobItem -> Mono.just("SUCCESSFUL"))
				.doOnSuccess(uploadStatus -> log.info("Upload Successful"))
				.doOnError(e -> log.error("Upload error {}", e.getMessage()));
	}

	public Flux<ByteBuffer> download(String blobIdentifier) {
		return blobServiceAsyncClient
				.getBlobContainerAsyncClient(containerName)
				.getBlobAsyncClient(blobIdentifier)
				.downloadStream()
				.doOnError(e -> log.error("blob download error {}",e.getMessage()));
	}
}