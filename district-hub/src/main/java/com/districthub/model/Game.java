package com.districthub.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Game {
    private String id;
    private String gameDate;
    private String gameTime;
    private String homeTeam;
    private String awayTeam;
    private String sport;
    private String level;
    private String location;
    private Integer homeScore;
    private Integer awayScore;
    private String status;
    private String sources;
    private String createdAt;
    private String updatedAt;
    private boolean manual; // true = user-authored; never overwritten by a platform sync pull

    public Game() {}

    public Game(String id, String gameDate, String gameTime, String homeTeam, String awayTeam,
                String sport, String level, String location, Integer homeScore, Integer awayScore,
                String status, String sources, String createdAt, String updatedAt) {
        this.id = id;
        this.gameDate = gameDate;
        this.gameTime = gameTime;
        this.homeTeam = homeTeam;
        this.awayTeam = awayTeam;
        this.sport = sport;
        this.level = level;
        this.location = location;
        this.homeScore = homeScore;
        this.awayScore = awayScore;
        this.status = status;
        this.sources = sources;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public List<String> getSourceList() {
        if (sources == null || sources.isEmpty()) return new ArrayList<>();
        return Arrays.asList(sources.split(","));
    }

    public void addSource(String platform) {
        if (platform == null || platform.isEmpty()) return;
        List<String> list = new ArrayList<>(getSourceList());
        if (!list.contains(platform)) {
            list.add(platform);
            sources = String.join(",", list);
        }
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getGameDate() { return gameDate; }
    public void setGameDate(String gameDate) { this.gameDate = gameDate; }

    public String getGameTime() { return gameTime; }
    public void setGameTime(String gameTime) { this.gameTime = gameTime; }

    public String getHomeTeam() { return homeTeam; }
    public void setHomeTeam(String homeTeam) { this.homeTeam = homeTeam; }

    public String getAwayTeam() { return awayTeam; }
    public void setAwayTeam(String awayTeam) { this.awayTeam = awayTeam; }

    public String getSport() { return sport; }
    public void setSport(String sport) { this.sport = sport; }

    public String getLevel() { return level; }
    public void setLevel(String level) { this.level = level; }

    public String getLocation() { return location; }
    public void setLocation(String location) { this.location = location; }

    public Integer getHomeScore() { return homeScore; }
    public void setHomeScore(Integer homeScore) { this.homeScore = homeScore; }

    public Integer getAwayScore() { return awayScore; }
    public void setAwayScore(Integer awayScore) { this.awayScore = awayScore; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getSources() { return sources; }
    public void setSources(String sources) { this.sources = sources; }

    public String getCreatedAt() { return createdAt; }
    public void setCreatedAt(String createdAt) { this.createdAt = createdAt; }

    public String getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(String updatedAt) { this.updatedAt = updatedAt; }

    public boolean isManual() { return manual; }
    public void setManual(boolean manual) { this.manual = manual; }
}
