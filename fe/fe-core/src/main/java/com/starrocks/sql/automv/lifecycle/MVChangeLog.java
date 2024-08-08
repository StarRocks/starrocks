// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.automv.lifecycle;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;
import com.starrocks.epack.persist.EditLogEPack;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.automv.generator.MVName;
import com.starrocks.sql.automv.util.TieredList;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class MVChangeLog implements Writable {
    @SerializedName(value = "mvName")
    private final MVName mvName;
    @SerializedName(value = "entries")
    private final TieredList<Entry> entries;

    public MVChangeLog(MVName mvName, TieredList<Entry> latestEntries) {
        this.mvName = mvName;
        this.entries = latestEntries;
    }

    public static MVChangeLog read(DataInput input) throws IOException {
        String s = Text.readString(input);
        return GsonUtils.GSON.fromJson(s, MVChangeLog.Builder.class).build();
    }

    public static MVChangeLog stagingTenured(MVName name) {
        return new MVChangeLog(name, TieredList.genesis()).addNewEntry(MVPhase.MP_TENURED);
    }

    public static MVChangeLog genesis(MVName mvName) {
        return new MVChangeLog(mvName, TieredList.genesis());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public MVName getMVName() {
        return mvName;
    }

    public TieredList<Entry> getEntries() {
        return entries;
    }

    public MVChangeLog addNewEntry(MVPhase phase) {
        Preconditions.checkArgument(!entries.isEmpty() || phase == MVPhase.MP_CRADLE);
        MVPhase prevPhase = entries.isEmpty() ? phase : entries.get(-1).phase;
        Entry newEntry = new Entry(System.currentTimeMillis(), prevPhase, phase);
        TieredList<Entry> newEntries = entries.concatOne(newEntry).tail(10);
        return new MVChangeLog(mvName, newEntries);
    }

    public Entry getLatestEntry() {
        Preconditions.checkArgument(!entries.isEmpty());
        return entries.get(-1);
    }

    public Optional<Entry> getPenultimateEntry() {
        if (entries.size() >= 2) {
            return Optional.of(entries.get(-2));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MVChangeLog that = (MVChangeLog) o;
        return Objects.equals(mvName, that.mvName) && Objects.equals(entries, that.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(mvName, entries);
    }

    @Override
    public String toString() {
        return "MVChangeLog{" +
                "mvName=" + mvName +
                ", entries=" + entries +
                '}';
    }

    public void persist() {
        EditLogEPack editLog = (EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog();
        editLog.logMVChangeLog(this);
    }

    public static final class Builder {
        @SerializedName(value = "mvName")
        private MVName mvName;
        @SerializedName(value = "entries")
        private List<Entry> entries;

        public MVName getMvName() {
            return mvName;
        }

        public void setMvName(MVName mvName) {
            this.mvName = mvName;
        }

        public List<Entry> getEntries() {
            return entries;
        }

        public void setEntries(List<Entry> entries) {
            this.entries = entries;
        }

        public MVChangeLog build() {
            return new MVChangeLog(mvName, TieredList.<Entry>newGenesisTier().addAll(entries).build());
        }
    }

    public static final class Entry {
        @SerializedName(value = "enterTime")
        private final long enterTime;
        @SerializedName(value = "prevPhase")
        private final MVPhase prevPhase;
        @SerializedName(value = "phase")
        private final MVPhase phase;

        public Entry(long enterTime, MVPhase prevPhase, MVPhase phase) {
            this.enterTime = enterTime;
            this.prevPhase = prevPhase;
            this.phase = phase;
        }

        public long getEnterTime() {
            return enterTime;
        }

        public MVPhase getPhase() {
            return phase;
        }

        public MVPhase getPrevPhase() {
            return prevPhase;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Entry entry = (Entry) o;
            return enterTime == entry.enterTime && prevPhase == entry.prevPhase &&
                    phase == entry.phase;
        }

        @Override
        public int hashCode() {
            return Objects.hash(enterTime, prevPhase, phase);
        }

        @Override
        public String toString() {
            return "Entry{" +
                    "enterTime=" + enterTime +
                    ", prevPhase=" + prevPhase +
                    ", phase=" + phase +
                    '}';
        }

    }
}