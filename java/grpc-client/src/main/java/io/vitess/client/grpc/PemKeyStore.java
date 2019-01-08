package io.vitess.client.grpc;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.RSAPrivateCrtKeySpec;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.DatatypeConverter;
import org.bouncycastle.asn1.ASN1Primitive;
import org.bouncycastle.asn1.ASN1Sequence;
import org.bouncycastle.asn1.pkcs.RSAPrivateKey;

public class PemKeyStore {
  private final List<Certificate> certificates = new ArrayList<>();
  private final List<KeySpec> keys = new ArrayList<>();

  private KeyStore newEmptyKeyStore(char[] password)
      throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
    KeyStore result = KeyStore.getInstance(KeyStore.getDefaultType());
    result.load(null, password);
    return result;
  }

  private Certificate decodeCertificate(byte[] bytes) throws CertificateException {
    CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
    return certificateFactory.generateCertificate(new ByteArrayInputStream(bytes));
  }

  private KeySpec convertPKCS1toPKCS8(byte[] pkcs1Key) throws IOException {
    ASN1Primitive keyObject = ASN1Sequence.fromByteArray(pkcs1Key);
    RSAPrivateKey rsaPrivateKey = RSAPrivateKey.getInstance(keyObject);

    return new RSAPrivateCrtKeySpec(
        rsaPrivateKey.getModulus(),
        rsaPrivateKey.getPublicExponent(),
        rsaPrivateKey.getPrivateExponent(),
        rsaPrivateKey.getPrime1(),
        rsaPrivateKey.getPrime2(),
        rsaPrivateKey.getExponent1(),
        rsaPrivateKey.getExponent2(),
        rsaPrivateKey.getCoefficient()
    );
  }

  private byte[] decodeBase64Until(BufferedReader reader, String until) throws IOException {
    StringBuilder result = new StringBuilder();

    String line;
    while ((line = reader.readLine()) != null) {
      if (line.isEmpty()) continue;
      if (line.matches(until)) {
        try {
          return DatatypeConverter.parseBase64Binary(result.toString());
        } catch (IllegalArgumentException e) {
          throw new IOException("invalid base64");
        }
      }
      result.append(line);
    }
    throw new IOException(until + " not found");
  }

  public void load(File file) throws IOException, CertificateException {
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      load(reader);
    }
  }

  public void load(BufferedReader reader) throws IOException, CertificateException {
    String line;
    while ((line = reader.readLine()) != null) {
      if (line.matches("-+BEGIN CERTIFICATE-+")) {
        byte[] bytes = decodeBase64Until(reader, "-+END CERTIFICATE-+");
        certificates.add(decodeCertificate(bytes));
      } else if (line.matches("-+BEGIN RSA PRIVATE KEY-+")) {
        byte[] bytes = decodeBase64Until(reader, "-+END RSA PRIVATE KEY-+");
        keys.add(convertPKCS1toPKCS8(bytes));
      } else if (line.matches("-+BEGIN PRIVATE KEY-+")) {
        byte[] bytes = decodeBase64Until(reader, "-+END PRIVATE KEY-+");
        keys.add(new PKCS8EncodedKeySpec(bytes));
      } else if (line.trim().isEmpty()) {
        // This is ok, just keep going
      } else {
        throw new IOException("unexpected line: " + line);
      }
    }
  }

  public KeyStore toKeyStore(String keyAlias, char[] keystorePassword, char[] keyPassword)
      throws NoSuchAlgorithmException, CertificateException, KeyStoreException, IOException,
      InvalidKeySpecException {
    KeyStore keyStore = newEmptyKeyStore(keystorePassword);
    if (keys.size() > 0) {
      KeyFactory keyFactory = KeyFactory.getInstance("RSA");
      Key privateKey = keyFactory.generatePrivate(keys.get(0));
      keyStore.setKeyEntry(keyAlias, privateKey, keyPassword != null ? keyPassword : new char[0],
          certificates.toArray(new Certificate[0]));
    }
    return keyStore;
  }
}
