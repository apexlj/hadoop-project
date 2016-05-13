package com.sq.historyTrackMapreduce.model;

/**
 * 经纬度对象
 *
 * @author lijiang
 * @version 1.0
 * @since 2016/5/10 18:56
 */
public class LngLat implements Cloneable {
    private final static double Rc = 6378137;
    private final static double Rj = 6356725;
    private double m_Longitude, m_Latitude;
    private double m_RadLo, m_RadLa;
    private double Ec;
    private double Ed;

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public LngLat() {
    }

    public LngLat(double longitude, double latitude) {
        calc(longitude, latitude);
    }

    public void set(double longitude, double latitude) {
        calc(longitude, latitude);
    }

    private void calc(double longitude, double latitude) {
        m_Longitude = longitude;
        m_Latitude = latitude;
        m_RadLo = longitude * Math.PI / 180.;
        m_RadLa = latitude * Math.PI / 180.;
        Ec = Rj + (Rc - Rj) * (90. - m_Latitude) / 90.;
        Ed = Ec * Math.cos(m_RadLa);
    }

    /**
     * lngLat与此经纬度之间连线与正北的方向角
     *
     * @param lngLat
     * @return
     */
    public double getAngle(LngLat lngLat) {
        double dx = (lngLat.m_RadLo - this.m_RadLo) * this.Ed;
        double dy = (lngLat.m_RadLa - this.m_RadLa) * this.Ec;
        double angle = 0.0;
        angle = Math.atan(Math.abs(dx / dy)) * 180. / Math.PI;
        double dLo = lngLat.m_Longitude - this.m_Longitude;
        double dLa = lngLat.m_Latitude - this.m_Latitude;
        if (dLo > 0 && dLa <= 0) {
            angle = (90. - angle) + 90;
        } else if (dLo <= 0 && dLa < 0) {
            angle = angle + 180.;
        } else if (dLo < 0 && dLa >= 0) {
            angle = (90. - angle) + 270;
        }
        return angle;
    }

    @Override
    public String toString() {
        return "LngLat{" +
                "Latitude=" + m_Latitude +
                ", Longitude=" + m_Longitude +
                '}';
    }
}
