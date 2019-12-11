package com.datalogs.crypto;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.nio.file.Files;
import java.security.*;
import java.util.*;

/**
 * Created by user-1 on 2/10/17.
 */
public class FileCrypto {

    public enum CryptoAction {
        ENCRYPT,
        DECRYPT
    }

    public static void fileCrypt(CryptoAction action,
                                 String inFile,
                                 String outFile,
                                 String keyFile) {

        BigCrypto aBigCrypto = null;

        boolean encrypting = false;

        try {

            byte[] inBuffer = new byte[8 * 1024];
            // Input Buffer

            byte[] outBuffer = null;
            // Output Buffer

            boolean isInitialized = false;
            long totalSize = 0;
            // Total Read Bytes

            int readBytes = 0;
            // Auxiliary variable

            switch (action) {
                case ENCRYPT:
                    aBigCrypto = new BigCrypto(keyFile, null);
                    encrypting = true;
                    break;
                case DECRYPT:
                    aBigCrypto = new BigCrypto(null, keyFile);

            }

            // Opens The input file and generates a new stream
            System.out.print("* Opening Input File (" + inFile + ") ... ");
            FileInputStream fin = new FileInputStream(new File(inFile));
            System.out.println("Ok");

            // Opens the output file and generates a new stream
            System.out.print("* Opening Output File (" + outFile + ") ... ");
            FileOutputStream fout = new FileOutputStream(new File(outFile));
            System.out.println("Ok");

            if (encrypting) {

                //
                // Encrypts the Input File
                //

                System.out.println("* Encrypting Data ... ");

                // Cycle through the input file and writes the
                // encrypted data to the output file
                while ((readBytes = fin.read(inBuffer)) > -1) {

                    // If we have not done the initialization, let's
                    // do it now
                    if (isInitialized == false) {

                        // Decrypts the input and writes the output (if any)
                        if ((outBuffer = aBigCrypto.encryptInit(inBuffer, 0, readBytes)) != null) {
                            fout.write(outBuffer);
                        }

                        // Initialization
                        isInitialized = true;

                        // Updates the totalSize
                        totalSize = readBytes;

                        // Read new Data
                        continue;
                    }

                    // Updates the totalSize
                    totalSize += readBytes;

                    // Here we do need to update the encryption
                    if ((outBuffer = aBigCrypto.encryptUpdate(inBuffer, 0, readBytes)) != null) {
                        fout.write(outBuffer);
                    }

                }

                // Now we just need to finalize the encryption
                if ((outBuffer = aBigCrypto.encryptFinal()) != null)
                    fout.write(outBuffer);

            } else {

                //
                // Decrypts the Input File
                //

                System.out.print("* Decrypting Data ... ");

                // Cycle Through the Input file bytes
                while ((readBytes = fin.read(inBuffer)) > -1) {

                    // If we have not initialized the decryption, let's
                    // do it now
                    if (isInitialized == false) {

                        // If we have output from the initialization, let's
                        // save it to the output stream
                        if ((outBuffer = aBigCrypto.decryptInit(inBuffer, 0, readBytes)) != null) {
                            fout.write(outBuffer);
                        }
                        // Initialization
                        isInitialized = true;

                        // Updates the totalSize
                        totalSize = readBytes;

                        // Read new Data
                        continue;
                    }

                    // Updates the totalSize
                    totalSize += readBytes;

                    // Here we do need to update the decryption
                    if ((outBuffer = aBigCrypto.decryptUpdate(inBuffer, 0, readBytes)) != null) {
                        fout.write(outBuffer);
                    }
                }

                // Now we just need to finalize the decryption
                if ((outBuffer = aBigCrypto.decryptFinal()) != null)
                    fout.write(outBuffer);
            }

            System.out.println("Ok");

            // Let's now close the streams
            System.out.print("* Closing Input and Output Streams ... ");
            fin.close();
            fout.close();
            System.out.println("Ok");

            // All Done
            System.out.println("\r\nAll Done\r\n");
        } catch (Exception ex) {

            // Generic Stack Trace
            ex.printStackTrace();
        }
    }
}
