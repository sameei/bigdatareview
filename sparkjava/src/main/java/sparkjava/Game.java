package sparkjava;

import java.io.Serializable;

public class Game implements Serializable {
    private final int rank;
    private final String name;
    private final String publisher;
    private final String genre;

    public Game(int rank, String name, String publisher, String genre) {
        this.rank = rank;
        this.name = name;
        this.publisher = publisher;
        this.genre = genre;
    }

    public int getRank() {
        return rank;
    }

    public String getName() {
        return name;
    }

    public String getPublisher() {
        return publisher;
    }

    public String getGenre() {
        return genre;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Game game = (Game) o;

        if (rank != game.rank) return false;
        if (!name.equals(game.name)) return false;
        if (!publisher.equals(game.publisher)) return false;
        return genre.equals(game.genre);
    }

    @Override
    public int hashCode() {
        int result = rank;
        result = 31 * result + name.hashCode();
        result = 31 * result + publisher.hashCode();
        result = 31 * result + genre.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Game{" +
                "rank=" + rank +
                ", name='" + name + '\'' +
                ", publisher='" + publisher + '\'' +
                ", genre='" + genre + '\'' +
                '}';
    }
}
