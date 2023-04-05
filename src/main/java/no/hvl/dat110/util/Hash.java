package no.hvl.dat110.util;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash {

    private static MessageDigest md = null;

    public static BigInteger hashOf(String entity) {

        BigInteger hashint = null;


        try {
            md = MessageDigest.getInstance("MD5");
            byte[] byteArr = md.digest(entity.getBytes());
            String hex = toHex(byteArr);
            hashint = new BigInteger(hex, 16);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


        return hashint;
    }

    public static BigInteger addressSize() {

        BigInteger out = null;

        int length = bitSize();
        out = (new BigInteger("2").pow(length));
        return out;
    }

    public static int bitSize() {

        int digestlen = 0;

        try {
            digestlen = MessageDigest.getInstance("MD5").getDigestLength();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


        return digestlen * 8;
    }

    public static String toHex(byte[] digest) {
        StringBuilder strbuilder = new StringBuilder();
        for (byte b : digest) {
            strbuilder.append(String.format("%02x", b & 0xff));
        }
        return strbuilder.toString();
    }

}