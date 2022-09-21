package org.example;

import java.io.Serializable;
import java.sql.Timestamp;

public class Log implements Serializable {

    Timestamp timeCreate;
    Timestamp cookieCreate;
    int browserCode;
    String browserVer;
    int osCode;
    String osVer;
    long ip;
    int locId;
    String domain;
    int siteId;
    int cId;
    String path;
    String referer;
    long guid;
    String flashVersion;
    String jre;
    String sr;
    String sc;
    int geographic;
    String category;

    public Log() {

    }

    public Log(Timestamp timeCreate, Timestamp cookieCreate, int browserCode, String browserVer, int osCode, String osVer, long ip, int locId, String domain, int siteId, int cId, String path, String referer, long guid, String flashVersion, String jre, String sr, String sc, int geographic, String category) {
        this.timeCreate = timeCreate;
        this.cookieCreate = cookieCreate;
        this.browserCode = browserCode;
        this.browserVer = browserVer;
        this.osCode = osCode;
        this.osVer = osVer;
        this.ip = ip;
        this.locId = locId;
        this.domain = domain;
        this.siteId = siteId;
        this.cId = cId;
        this.path = path;
        this.referer = referer;
        this.guid = guid;
        this.flashVersion = flashVersion;
        this.jre = jre;
        this.sr = sr;
        this.sc = sc;
        this.geographic = geographic;
        this.category = category;
    }

    public Timestamp getTimeCreate() {
        return timeCreate;
    }

    public void setTimeCreate(Timestamp timeCreate) {
        this.timeCreate = timeCreate;
    }

    public Timestamp getCookieCreate() {
        return cookieCreate;
    }

    public void setCookieCreate(Timestamp cookieCreate) {
        this.cookieCreate = cookieCreate;
    }

    public int getBrowserCode() {
        return browserCode;
    }

    public void setBrowserCode(int browserCode) {
        this.browserCode = browserCode;
    }

    public String getBrowserVer() {
        return browserVer;
    }

    public void setBrowserVer(String browserVer) {
        this.browserVer = browserVer;
    }

    public int getOsCode() {
        return osCode;
    }

    public void setOsCode(int osCode) {
        this.osCode = osCode;
    }

    public String getOsVer() {
        return osVer;
    }

    public void setOsVer(String osVer) {
        this.osVer = osVer;
    }

    public long getIp() {
        return ip;
    }

    public void setIp(long ip) {
        this.ip = ip;
    }

    public int getLocId() {
        return locId;
    }

    public void setLocId(int locId) {
        this.locId = locId;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public int getSiteId() {
        return siteId;
    }

    public void setSiteId(int siteId) {
        this.siteId = siteId;
    }

    public int getcId() {
        return cId;
    }

    public void setcId(int cId) {
        this.cId = cId;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getReferer() {
        return referer;
    }

    public void setReferer(String referer) {
        this.referer = referer;
    }

    public long getGuid() {
        return guid;
    }

    public void setGuid(long guid) {
        this.guid = guid;
    }

    public String getFlashVersion() {
        return flashVersion;
    }

    public void setFlashVersion(String flashVersion) {
        this.flashVersion = flashVersion;
    }

    public String getJre() {
        return jre;
    }

    public void setJre(String jre) {
        this.jre = jre;
    }

    public String getSr() {
        return sr;
    }

    public void setSr(String sr) {
        this.sr = sr;
    }

    public String getSc() {
        return sc;
    }

    public void setSc(String sc) {
        this.sc = sc;
    }

    public int getGeographic() {
        return geographic;
    }

    public void setGeographic(int geographic) {
        this.geographic = geographic;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    @Override
    public String toString() {
        return "Log{" +
                "timeCreate=" + timeCreate +
                ", cookieCreate=" + cookieCreate +
                ", browserCode=" + browserCode +
                ", browserVer='" + browserVer + '\'' +
                ", osCode=" + osCode +
                ", osVer='" + osVer + '\'' +
                ", ip=" + ip +
                ", locId=" + locId +
                ", domain='" + domain + '\'' +
                ", siteId=" + siteId +
                ", cId=" + cId +
                ", path='" + path + '\'' +
                ", referer='" + referer + '\'' +
                ", guid=" + guid +
                ", flashVersion='" + flashVersion + '\'' +
                ", jre='" + jre + '\'' +
                ", sr='" + sr + '\'' +
                ", sc='" + sc + '\'' +
                ", geographic=" + geographic +
                ", category=" + category +
                '}';
    }
}
