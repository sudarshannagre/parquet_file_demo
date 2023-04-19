package com.sud.parquet_file_demo.util;

import java.util.Base64;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.encoders.Hex;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BouncyCastleEncryption {

	private static final String CHARSET = "UTF-8";
	private static final String HMAC_SHA256_ALGORITH = "HmacSHA256";

	public static Mac getMac(String key) {
		try {
			Mac hmac = Mac.getInstance(HMAC_SHA256_ALGORITH, new BouncyCastleProvider());

			SecretKeySpec secretKey = new SecretKeySpec(Base64.getDecoder().decode(key), HMAC_SHA256_ALGORITH);

			hmac.init(secretKey);

			return hmac;
		} catch (Exception e) {
			e.getMessage();
		}
		return null;
	}

	public static String getEncryption(String originalString, Mac hmac) {
		try {
			byte[] resultHmac = hmac.doFinal(originalString.getBytes(CHARSET));

			return Hex.toHexString(resultHmac);
		} catch (Exception e) {
			e.getMessage();
		}
		return null;
	}

}
