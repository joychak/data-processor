package com.datalogs.crypto;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;

import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.PrivateKey;
import java.security.KeyFactory;
import java.security.spec.*;
import java.util.Arrays;
import java.security.Provider;
import java.security.Security;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.*;

public class BigCrypto implements Serializable {

    private static byte[] getKeyAsBytes(String keyFileName) throws IOException {

        byte[] bytes = null;
        if (keyFileName != null) {
            Path keyPath = new File(keyFileName).toPath();
            bytes = Files.readAllBytes(keyPath);
        }
        return bytes;
    }

    // ===========
    // Constructor
    // ===========
    public BigCrypto(String publicKeyFileName, String privateKeyFileName) throws Exception {
        this(getKeyAsBytes(publicKeyFileName), getKeyAsBytes(privateKeyFileName));
    }

    // (if filename is provided)
    public BigCrypto(byte[] publicKeyAsBytes, byte[] privateKeyAsBytes) throws Exception {

        Cipher aTestCipher = Cipher.getInstance("AES");
        // Get the AES cipher to query for max allowed size

        // Checks for the size of the max allowed key size
        if (Cipher.getMaxAllowedKeyLength("AES/CBC/PKCS5Padding") < 256)
            throw new Exception("Please Enable Unlimited Crypto");

        // Loads the public key and assigns it to the instance
        if (publicKeyAsBytes != null) loadPublicKey(publicKeyAsBytes);

        // Loads the public key and assigns it to the instance
        if (privateKeyAsBytes != null) loadPrivateKey(privateKeyAsBytes);

        // Secure Random
        sRand = SecureRandom.getInstanceStrong();

        // Instance Variables
        aesCipher = null;

        // Decrypt Specific
        buffer       = null;
        headerParsed = false;
        ivParsed     = false;

    }

    // ==============
    // Public Methods
    // ==============

    public byte[] encryptInit(byte[] data, int from, int to) {

        return encryptInit(Arrays.copyOfRange(data, from, to));

    }

    public byte[] encryptInit(byte[] data) {

        byte[] ret = null;
        // Encrypted data return

        byte[] encData = null;
        // Encrypted Data container (minus the header and IV)

        byte[] ivData = null;
        // Initialization Vector Array

        byte[] encodedKey = null;
        // Encryption Key

        try {

            // If the instance does not have a public key loaded
            // we return an error
            if (this.pubKey == null) throw new Exception("No Public Key loaded");

            // Get the Cipher Instance
            this.aesCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");

            // Generates the IV
            ivData = new byte[aesCipher.getBlockSize()];

            // Fills the IV data
            sRand.nextBytes(ivData);

            // Now Generates the IV Specs to be used in the cipher init
            IvParameterSpec ivSpecs = new IvParameterSpec(ivData);

            // Encrypts the encodedKey (AES) with the recipientKey (RSA)
            if ((encodedKey = encodeOctetString(
                    publicKeyEncrypt(this.pubKey, genAESKey(256)))) == null) {
                // Error Condition when encrypting (and encoding) the AES key
                throw new Exception("Error Encrypting for Recipient RSA key");
            }

            // Initializes the Cipher with the aesKey and ivSpecs
            aesCipher.init(Cipher.ENCRYPT_MODE, this.aesKey, ivSpecs);

            // If we have data, let's encrypt it
            if (data != null && data.length > 0) encData = aesCipher.update(data);

            // We now need to combine the different byte[] into a single return object
            if (encData != null) {
                ret = new byte[encodedKey.length + ivData.length + encData.length];
            } else {
                ret = new byte[encodedKey.length + ivData.length];
            }

            // Copy the encrypted AES key
            System.arraycopy(encodedKey, 0, ret, 0, encodedKey.length);

            // Copy the IV data
            System.arraycopy(ivData, 0, ret, encodedKey.length, ivData.length);

            // Copy the encrypted data (if any)
            if (encData != null) {
                System.arraycopy(encData, 0, ret, encodedKey.length + ivData.length, encData.length);
            }

            // Return the encrypted data
            return ret;

        } catch (Exception ex) {

            // Error Handling
            ex.printStackTrace();
        }

        // Error Condition
        return null;
    }

    public byte[] encryptUpdate(byte[] data, int from, int to) {

        return encryptUpdate(Arrays.copyOfRange(data, from, to));

    }

    public byte[] encryptUpdate(byte[] data) {

        byte[] ret = null;
        // Encrypted data return

        // Checks the input and the status
        if (aesCipher == null || data == null) return null;

        // Let's encrypt the data and return the encrypted bytes
        ret = aesCipher.update(data);

        // Return encrypted data
        return ret;
    }

    public byte[] encryptFinal() {

        byte[] ret = null;
        // Return Array

        // Checks the status
        if (aesCipher == null) return null;

        // Let's finalize and return the encrypted data
        try {

            // Finalizes the Encryption
            ret = aesCipher.doFinal();

            // Resets the Cipher
            aesCipher = null;

            // Returns the encrypted data (if any)
            return ret;

        } catch (Exception ex) {

            // Error Handling
            ex.printStackTrace();
        }

        // Error Condition
        return null;
    }

    public byte[] decryptInit(byte[] data, int from, int to) throws Exception {

        return decryptInit(Arrays.copyOfRange(data, from, to));
    }

    public byte[] decryptInit(byte[] data) throws Exception {

        byte[] ret = null;
        // Return Array of Decrypted Data

        // Input Checks
        if (data == null) throw new Exception("No data to encrypt");

        // Checks the status
        if (privKey == null) throw new Exception("No Private key for decryption");

        // Copy the data in the buffer
        buffer = data;

        // Reset the internal variables
        headerParsed = false;
        ivParsed     = false;

        // Calls the internal decryptInitUpdate(). If successful, we shall
        // decrypt the actual encrypted data (if any is left after parsing
        // the encryption header and the IV)
        if (true == decryptInitUpdate()) ret = decryptUpdate(null);

        // Success
        return ret;
    }

    public byte[] decryptUpdate(byte[] data, int from, int to) throws Exception {

        return decryptUpdate(Arrays.copyOfRange(data, from, to));

    }

    public byte[] decryptUpdate(byte[] data) throws Exception {

        byte[] decBuffer = null;
        byte[] decData = null;
        byte[] ret = null;
        // Return Array

        // Checks the internal status
        if (decryptInitUpdate() == false) {

            // Checks the status of the internal buffer
            if (buffer == null || buffer.length > 4096) {

                // Either the buffer does not exists or it reached the maximum size
                // where we are sure the header and IV should have fitted
                throw new Exception("Decryption Can Not Be Properly Initialized");
            }

            // We do not have enough data to actually decrypt yet. Let's return null
            return null;
        }

        // Here we can actually do the decryption
        if (buffer != null && buffer.length > 0) decBuffer = aesCipher.update(buffer);

        // If no data was passed, we just return the decodedBuffer (if any)
        if (data == null) {

            // Assigns the decrypted buffer to return variable
            ret = decBuffer;

            // Resets the buffer
            buffer = null;

            // Returns the result
            return ret;
        }

        // We need to process the passed data
        decData = aesCipher.update(data);

        // If no encrypted buffer, let's just return the encrypted data
        if (decBuffer == null) return decData;

        // Let's combine the two encryptions (buffer + data)

        // Allocates the return buffer
        ret = new byte[decBuffer.length + decData.length];

        // Copy the decrypted buffer
        System.arraycopy(decBuffer, 0, ret, 0, decBuffer.length);

        // Copy the decrypted data
        System.arraycopy(decData, 0, ret, decBuffer.length, decData.length);

        // Returns the Data
        return ret;
    }

    public byte[] decryptFinal() throws Exception {

        byte[] ret = null;
        // Return Array

        // Checks the internal status
        if (decryptInitUpdate() == false) {

            // If this happens, we can not even parse the header
            // and the IV - we throw an exception here
            throw new Exception("Decryption Not Initialized");
        }

        // If we still have data in the internal buffer, let's decrypt that
        if (buffer != null && buffer.length > 0) ret = aesCipher.doFinal(buffer);
        else ret = aesCipher.doFinal();

        // Let's reset the internal variables
        aesCipher    = null;
        ivParsed     = false;
        headerParsed = false;
        buffer       = null;

        // Return the final chunk of decrypted data (if any)
        return ret;
    }

    // =================
    // Auxiliary Methods
    // =================

    // Load Private Key (PKCS#8 DER encoded)
    public void loadPrivateKey(byte[] privateKeyAsBytes) throws Exception {

        KeyFactory kf = KeyFactory.getInstance("RSA");
        // Key Factory

        byte[] keyData = privateKeyAsBytes;
        // Raw Bytes array with Key Data

        // Parses the Key
        PKCS8EncodedKeySpec keySpecs = new PKCS8EncodedKeySpec(keyData);

        // Generates the PrivateKey Object and returns it
        this.privKey = kf.generatePrivate(keySpecs);

        // Success
    }

    // Load a Public Key (PKCS#1 DER encoded)
    public void loadPublicKey(byte[] publicKeyAsBytes) throws Exception {

        KeyFactory kf = KeyFactory.getInstance("RSA");
        // Key Factory

        byte[] keyData = publicKeyAsBytes;

        X509EncodedKeySpec keySpecs = new X509EncodedKeySpec(keyData);
        // Creates the KeySpecs to be able to generate a new Key

        // Assigns the Key to the instance
        this.pubKey = kf.generatePublic(keySpecs);
    }

    // ===============
    // Private Methods
    // ===============

    private byte[] publicKeyEncrypt(PublicKey key, byte[] data) {

        try {

            Cipher aCipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
            // Cipher Object for RSA w/ OAEP

            // Checks for the Public Key to use for the Encryption
            if (key == null) throw new Exception("No Public Key");

            // Checks we have the data to encrypt
            if (data == null || data.length == 0) throw new Exception("No Data to Encrypt");

            // Initializes the Object for Encryption
            aCipher.init(Cipher.ENCRYPT_MODE, key);

            // Encrypts the data and saves it in cipherText
            return aCipher.doFinal(data);

        } catch (Exception ex) {

            // Exception Handling
            ex.printStackTrace();
        }

        // Error Case
        return null;
    }

    private byte[] privateKeyDecrypt(PrivateKey key, byte[] data) throws Exception {

        byte[] decText = null;

        try {

            Cipher aCipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
            // Cipher for RSA w/ OAEP padding scheme

            // Initializes the Cipher for Decryption
            aCipher.init(Cipher.DECRYPT_MODE, key);

            // Decrypts the data and returns it into decText
            decText = aCipher.doFinal(data);

            // Success
            return decText;

        } catch (Exception ex) {

            // Exception Handling
            ex.printStackTrace();
        }

        // Error Case
        return null;
    }

    private boolean decryptInitUpdate() throws Exception {

        // Checks if initialization was successful
        if (headerParsed == true && ivParsed == true) return true;

        // Return if we do not have data to process
        if (buffer == null) return false;

        // Loads the header (if not already parsed)
        if (headerParsed == false) {

            byte[] header = null;
            // Encrypted AES key data

            byte[] aesKeyData = null;
            // Decrypted AES key data

            // Try to read the header
            if ((header = this.decodeOctetString(buffer)) == null) return false;

            // Let's resize the buffer to carry only the data (since we parsed
            // the OCTET STRING successfully)
            buffer = Arrays.copyOfRange(buffer, getEncodedOctetStringNextByte(buffer), buffer.length);

            // Here we got the header, let's decrypt it
            if ((aesKeyData = this.privateKeyDecrypt(privKey, header)) == null)
                throw new Exception("Can not decrypt encryption header");

            // Key Generator
            KeyGenerator keygen = KeyGenerator.getInstance("AES");

            // Use the Secure Random
            keygen.init(sRand);

            // Generates the instance' SecretKey object
            aesKey = new SecretKeySpec(aesKeyData, "AES");

            // Updates the flag for the header
            headerParsed = true;
        }

        // Checks for the IV parsing
        if (headerParsed == true && ivParsed == false && buffer.length >= 16) {

            // Let's get the IV array
            byte[] ivData = Arrays.copyOf(buffer, 16);

            // Let's generate the IV Spec
            IvParameterSpec ivSpecs = new IvParameterSpec(ivData);

            // Now we can finally initialize
            // Let's now initilize the aesCipher
            aesCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            aesCipher.init(Cipher.DECRYPT_MODE, aesKey, ivSpecs);

            // Let's now move the rest of the buffer data in front
            // of the byte[] array
            if (buffer.length == 16) {

                // No more data to process
                buffer = null;

            } else {

                byte[] tmp = null;
                // Temporary byte buffer

                // Allocates a new buffer
                tmp = new byte[buffer.length - 16];

                // Copy the remaining data to the temporary buffer
                System.arraycopy(buffer, 16, tmp, 0, tmp.length);

                // Use the new array as buffer
                buffer = tmp;
            }

            // Updates the IV initialization flag
            ivParsed = true;
        }

        // Returns the status to the caller
        return (headerParsed && ivParsed);

    }

    private byte[] genAESKey(int size) {

        try {
            // Key Generator
            KeyGenerator keygen = KeyGenerator.getInstance("AES");

            // Initializes the Key Generator
            keygen.init(size);

            // Use the Secure Random
            keygen.init(sRand);

            // Generates the instance' SecretKey object
            this.aesKey = keygen.generateKey();

            // Returns the byte[] encoded secret key
            return aesKey.getEncoded();

        } catch (Exception ex) {

            // Error Handling
            ex.printStackTrace();
        }

        // Error Condition
        return null;
    }

    private byte[] encodeOctetString(byte[] data) {

        int lenBytes = 0;
        // Number of bytes that encode the string length

        byte[] ret = null;
        // Return array (DER encoded OCTET STRING)

        // Checks the two cased: long or short encoding of the string length
        if (data.length > 127) lenBytes = (int) (Math.ceil((double) data.length / 256));
        else lenBytes = 0;

        // Instantiates a new array with the required size for DER encoding
        ret = new byte[2 + lenBytes + data.length];

        // Sets the OCTET STRING type
        ret[0] = 4;

        // Sets the OCTET STRING size
        if (data.length > 127) {

            // This is the case for long encoding format
            // and should accomodate for strings that are
            // 2^1008-1 bytes long.

            int tmp = data.length;
            // Auxiliary Value

            // Sets the Length of Length Bytes
            ret[1] = (byte) (128 + lenBytes);

            // Cycle Backward and shift each time
            for (int idx = lenBytes; idx > 0; idx--) {

                // Saves the lower 8 bits
                ret[1 + idx] = (byte) (tmp & 0xFF);
                tmp = tmp >> 8;
            }

        } else {

            // This is the case for short encoding format
            // that uses the size directly if the size is
            // less or equal to 127 octets long

            lenBytes = 0;
            ret[1] = (byte) (data.length & 127);
        }

        // Let's Copy the data from the original byte array
        // into the newly generated one
        System.arraycopy(data, 0, ret, 2 + lenBytes, data.length);

        // Returns the DER Encoded ASN1_OCTET_STRING
        return ret;
    }

    private byte[] decodeOctetString(byte[] data) {

        byte[] ret = null;
        // Return array

        int lenBytes = 0;
        // Number of Length Bytes that express the
        // size of the OCTET STRING

        int dataSize = 0;
        // Size of the OCTET STRING data

        // Let's return null if it is not an OCTET STRING
        if (data[0] != (byte) 4) return null;

        // Checks the Size
        if ((data[1] & 128) != 0) {
            // Long Encoding case, the next byte carries
            // the number of bytes that actually express
            // the length of the full string
            lenBytes = Byte.toUnsignedInt((byte) (data[1] & (byte) 0x7F));

            // We only support strings whose number of len bytes
            // would fix the int (limitation acceptable as this is
            // not a complete parser, but is restricted to a
            // specific case (usually the length is ~512bytes)
            if (lenBytes > 4) return null;

            // Cycle through the lenBytes to get the final
            // data size of the OCTET STRING
            for (int idx = 0; idx < lenBytes; idx++) {

                // Most Significant bits first. At each
                // step, we shift the dataSize << 8 bits
                // and then add the byte value to the size
                dataSize = dataSize << 8;
                dataSize += (int) Byte.toUnsignedInt(data[idx + 2]);
            }

        } else {
            // Short Encoding case, the next byte carries
            // the size of the string (which should be less
            // or equal to 127 bytes)
            dataSize = Byte.toUnsignedInt(data[2]);
        }

        // Checks the data array size is equal or larger than the
        // detected string size
        if (data.length < dataSize) return null;

        // Allocates the new array
        ret = new byte[dataSize];

        // Copy the contents from the original data array into the
        // return array
        System.arraycopy(data, 2 + lenBytes, ret, 0, dataSize);

        // Returns the OCTET STRING data
        return ret;
    }

    private int getEncodedOctetStringNextByte(byte[] data) {

        int lenBytes = 0;
        // Number of Length Bytes that express the
        // size of the OCTET STRING

        int dataSize = 0;
        // Size of the OCTET STRING data

        // Let's return null if it is not an OCTET STRING
        if (data[0] != (byte) 4) return -1;

        // Checks the Size
        if ((data[1] & 128) != 0) {
            // Long Encoding case, the next byte carries
            // the number of bytes that actually express
            // the length of the full string
            lenBytes = data[1] & (byte) 0x7F;

            // We only support strings whose number of len bytes
            // would fix the int (limitation acceptable as this is
            // not a complete parser, but is restricted to a
            // specific case (usually the length is ~512bytes)
            if (lenBytes > 4) return -1;

            // Cycle through the lenBytes to get the final
            // data size of the OCTET STRING
            for (int idx = 0; idx < lenBytes; idx++) {

                // Most Significant bits first. At each
                // step, we shift the dataSize << 8 bits
                // and then add the byte value to the size
                dataSize = dataSize << 8;
                dataSize += Byte.toUnsignedInt(data[idx + 2]);
            }

        } else {
            // Short Encoding case, the next byte carries
            // the size of the string (which should be less
            // or equal to 127 bytes)
            dataSize = Byte.toUnsignedInt(data[2]);
        }

        // Checks the data array size is equal or larger than the
        // detected string size
        if (data.length < dataSize) return -1;

        // Returns the total size
        return (2 + lenBytes + dataSize);
    }

    //
    // Some Private Instance Variables
    //
    private PrivateKey privKey;
    // Private Key Instance Variable

    private PublicKey pubKey;
    // Public Key Instance Variable

    private byte[] buffer;
    // Internal Buffer used for Header / IV parsing
    // during decryptionAPPT

    private Cipher aesCipher;
    // Cipher for Symmetric Crypto Operations

    private SecretKey aesKey;
    // Secret Key for Symmetric Crypto Operations

    private SecureRandom sRand;
    // Secure Random Generator

    private boolean headerParsed;
    // Flag for tracking the parsing of the encryption header

    private boolean ivParsed;
    // Flag for tracking the parsing of the IV

}
