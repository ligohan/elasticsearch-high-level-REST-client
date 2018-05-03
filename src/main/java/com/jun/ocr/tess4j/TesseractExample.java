package com.jun.ocr.tess4j;

import net.sourceforge.tess4j.ITesseract;
import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;
import org.junit.jupiter.api.Test;

import java.io.File;

public class TesseractExample {

    @Test
    public void demo() {
        File imageFile = new File("ocr-image/identity.jpg");
        ITesseract instance = new Tesseract();  // JNA Interface Mapping
         //ITesseract instance = new Tesseract1(); // JNA Direct Mapping

        try {
            String result = instance.doOCR(imageFile);
            System.out.println(result);
        } catch (TesseractException e) {
            System.err.println(e.getMessage());
        }
    }
}