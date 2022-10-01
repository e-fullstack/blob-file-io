package com.samitkumarpatel.blobfileio;

import com.azure.core.util.FluxUtil;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.models.AccessTier;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.options.BlobParallelUploadOptions;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.http.codec.multipart.Part;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.springframework.http.MediaType.APPLICATION_OCTET_STREAM;
import static org.springframework.http.MediaType.MULTIPART_FORM_DATA;
import static org.springframework.web.reactive.function.server.RequestPredicates.accept;
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

		return fileIO.findBlobByTag(blobIdentifier).next().flatMap(fileName -> {
			return ok()
					.contentType(APPLICATION_OCTET_STREAM)
					.header("Content-Disposition", "inline")
					.header("Content-Disposition", "attachment")
					.header("Content-Disposition", "attachment; filename=\"%s\"".formatted(fileName))
					.body(fileIO.download(fileName), ByteBuffer.class);
		});
	}

	public Mono<ServerResponse> upload(ServerRequest request) {
		return request.multipartData().flatMap(stringPartMultiValueMap -> {
			Map<String, Part> partMap = stringPartMultiValueMap.toSingleValueMap();
			var filePart = (FilePart) partMap.get("file");
			var fileName = filePart.filename().replace(" ","");
			var fileContent = filePart.content().flatMap(dataBuffer -> FluxUtil.toFluxByteBuffer(dataBuffer.asInputStream()));
			return fileIO.upload(fileName, fileContent)
					.flatMap(uploadMessage -> ok().body(Mono.just(uploadMessage), UploadResponse.class));
		});
	}
}

@Data @Builder @NoArgsConstructor @AllArgsConstructor
class UploadResponse {
	private String status;
	private String blobUrl;
	private String id;
}

/**
 * azure cloud sdk
 * https://learn.microsoft.com/en-us/java/api/com.azure.storage.blob.blobserviceasyncclient?view=azure-java-stable
 */
@Service
@RequiredArgsConstructor
@Slf4j
class FileIO {
	private final BlobContainerAsyncClient blobContainerAsyncClient;

	public Mono<UploadResponse> upload(String blobIdentifier, Flux<ByteBuffer> data) {
		long blockSize = 2 * 1024 * 1024;
		var uuid = UUID.randomUUID().toString();
		var fileName = blobIdentifier.replaceAll("[^a-zA-Z0-9\\.\\-]", "_");
		var blobAsyncClient = blobContainerAsyncClient.getBlobAsyncClient("%s_%s".formatted(uuid,fileName));
		return blobAsyncClient.uploadWithResponse(
						new BlobParallelUploadOptions(data)
								.setParallelTransferOptions(new ParallelTransferOptions().setBlockSizeLong(blockSize).setMaxConcurrency(2))
								/*.setHeaders(new BlobHttpHeaders() .setContentLanguage("en-US").setContentType("binary"))*/
								.setMetadata(Collections.singletonMap("metadata", "custom-metadata")).setTags(Collections.singletonMap("uuid", uuid))
								.setTier(AccessTier.HOT)
								.setRequestConditions(new BlobRequestConditions()/*.setLeaseId(leaseId)*/.setIfUnmodifiedSince(OffsetDateTime.now().minusDays(3)))
						)
				//.upload(data, new ParallelTransferOptions().setBlockSizeLong(blockSize).setMaxConcurrency(2), true)
				.map(blockBlobItem -> UploadResponse.builder().id(uuid).status("SUCCESS").blobUrl(blobAsyncClient.getBlobUrl()).build())
				.doOnSuccess(uploadStatus -> log.info("Upload Successful"))
				.doOnError(e -> log.error("Upload error {}", e.getMessage()));
	}
	public Flux<String> findBlobByTag(String blobIdentifier) {
		return blobContainerAsyncClient.findBlobsByTags("uuid='%s'".formatted(blobIdentifier)).map(blob -> {
			var blobName = blob.getName();
			log.info("{} = {}",blobIdentifier, blobName);
			return blobName;
		});
	}
	public Flux<ByteBuffer> download(String blobName) {
		log.info("Download Invocation for {}", blobName);
		return blobContainerAsyncClient
							.getBlobAsyncClient(blobName)
							.downloadStream()
							.doOnError(e -> new RuntimeException(e.getMessage()));
	}
}