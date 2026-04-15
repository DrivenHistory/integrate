package com.gameshub.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Team {
    private String id;
    private String name;
    private String sport;
    private String level;
    private String school;
    private String sources;

    public Team() {}

    public Team(String id, String name, String sport, String level, String school, String sources) {
        this.id = id;
        this.name = name;
        this.sport = sport;
        this.level = level;
        this.school = school;
        this.sources = sources;
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

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getSport() { return sport; }
    public void setSport(String sport) { this.sport = sport; }

    public String getLevel() { return level; }
    public void setLevel(String level) { this.level = level; }

    public String getSchool() { return school; }
    public void setSchool(String school) { this.school = school; }

    public String getSources() { return sources; }
    public void setSources(String sources) { this.sources = sources; }
}
