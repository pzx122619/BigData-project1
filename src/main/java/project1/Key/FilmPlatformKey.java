package project1.Key;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FilmPlatformKey implements WritableComparable<FilmPlatformKey> {
    Text filmId;
    Text platform;

    public FilmPlatformKey() {
        set(
                "",
                ""
        );
    }

    public FilmPlatformKey(String  filmId, String platform) {
        set(
                filmId,
                platform
        );
    }

    public void set(String filmId, String platform) {
        this.filmId = new Text(filmId);
        this.platform = new Text(platform);
    }

    public Text getFilmId() { return filmId; }
    public Text getPlatform() { return platform; }

    @Override
    public void write(DataOutput out) throws IOException {
        filmId.write(out);
        platform.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        filmId.readFields(in);
        platform.readFields(in);
    }

    @Override
    public int compareTo(FilmPlatformKey o) {
        int cmp = filmId.compareTo(o.filmId);
        if (cmp != 0) return cmp;
        return platform.compareTo(o.platform);
    }

    @Override
    public int hashCode() {
        return filmId.hashCode() * 163 + platform.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof FilmPlatformKey other) {
            return filmId.equals(other.filmId) && platform.equals(other.platform);
        }
        return false;
    }

    @Override
    public String toString() {
        return filmId + "," + platform;
    }
}