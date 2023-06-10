package com.validadorarquivo;

import java.util.Arrays;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Responsável por realizar a validação de um arquivo através do serviço lambda
 * da amazon
 * 
 * @author Rodrigo Fernandes
 *
 */
public class Handler implements RequestHandler<S3Event, String> {

	Gson gson = new GsonBuilder().setPrettyPrinting().create();

	/**
	 * Método a qual o serviço lambda irá chamar para validar o arquivo
	 */
	@Override
	public String handleRequest(S3Event event, Context context) {
		LambdaLogger logger = context.getLogger();

		var record = event.getRecords().get(0);
		String nomeObjeto = record.getS3().getObject().getUrlDecodedKey();
		String bucket = record.getS3().getBucket().getName();

		logger.log("OBJETO: " + nomeObjeto);
		logger.log("BUCKET: " + bucket);

		// Configurado dentro do serviço lambda os tipos que serão permitidos
		String[] tipos = System.getenv().get("tipos").split(",");

		var tipoObjeto = nomeObjeto.split("\\.")[1].toUpperCase();
		boolean valido = Arrays.stream(tipos).anyMatch(tipoObjeto::equals);

		if (!valido) {
			try {
				// inicializa o sdk para manipulação do s3
				AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

				// Cria a instancia do deleteObject para excluir no s3
				DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucket, nomeObjeto);
				s3Client.deleteObject(deleteObjectRequest);

				logger.log("======= OBJETO INVALIDO =======");
				logger.log("Objeto excluído com sucesso");

				return "Arquivo: " + nomeObjeto + " excluído com sucesso";

			} catch (Exception e) {
				logger.log(e.getMessage());
				throw new RuntimeException();
			}
		}

		logger.log("======= OBJETO VALIDO =======");
		return "Arquivo: " + nomeObjeto + " válido";
	}

}
