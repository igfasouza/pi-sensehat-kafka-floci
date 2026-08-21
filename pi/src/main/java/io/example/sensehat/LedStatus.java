package io.example.sensehat;

import com.pi4j.drivers.display.graphics.Argb32;
import com.pi4j.drivers.hat.raspberry.SenseHat;

final class LedStatus {

    private static final int WIDTH = 8;
    private static final int HEIGHT = 8;

    private static final double TEMP_MIN = 15.0;
    private static final double TEMP_MAX = 35.0;
    private static final double HUMIDITY_MIN = 0.0;
    private static final double HUMIDITY_MAX = 100.0;
    private static final double PRESSURE_MIN = 950.0;
    private static final double PRESSURE_MAX = 1050.0;

    private final SenseHat sense;
    private final int[] framebuffer = new int[WIDTH * HEIGHT];

    LedStatus(SenseHat sense) {
        this.sense = sense;
        sense.clear();
    }

    void render(double tempC, double humidityPct, double pressureMbar) {
        java.util.Arrays.fill(framebuffer, Argb32.BLACK);
        drawBar(0, 1, level(tempC, TEMP_MIN, TEMP_MAX), Argb32.fromRgb(200, 60, 30));
        drawBar(3, 4, level(humidityPct, HUMIDITY_MIN, HUMIDITY_MAX), Argb32.fromRgb(30, 90, 200));
        drawBar(6, 7, level(pressureMbar, PRESSURE_MIN, PRESSURE_MAX), Argb32.fromRgb(40, 180, 90));
        sense.setPixels(framebuffer);
    }

    void signalError() {
        sense.showCharacter('E', 255, 0, 0);
    }

    void close() {
        sense.clear();
    }

    private static int level(double value, double min, double max) {
        double clamped = Math.max(min, Math.min(max, value));
        return (int) Math.round(((clamped - min) / (max - min)) * HEIGHT);
    }

    private void drawBar(int xStart, int xEnd, int litRows, int color) {
        for (int y = 0; y < HEIGHT; y++) {
            boolean lit = (HEIGHT - y) <= litRows;
            int pixel = lit ? color : Argb32.BLACK;
            for (int x = xStart; x <= xEnd; x++) {
                framebuffer[y * WIDTH + x] = pixel;
            }
        }
    }
}
