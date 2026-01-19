-- Dejavu fingerprint database schema for MySQL
CREATE DATABASE IF NOT EXISTS dejavu_db;
USE dejavu_db;

-- Songs table
CREATE TABLE IF NOT EXISTS songs (
  song_id INT AUTO_INCREMENT PRIMARY KEY,
  song_name VARCHAR(250) NOT NULL,
  fingerprinted TINYINT DEFAULT 0,
  file_sha1 CHAR(40) NOT NULL,
  artist VARCHAR(250) DEFAULT '',
  album VARCHAR(250) DEFAULT '',
  genre VARCHAR(250) DEFAULT '',
  length INT DEFAULT 0,
  total_hashes INT NOT NULL DEFAULT 0,
  date_created DATETIME NOT NULL DEFAULT NOW(),
  date_modified DATETIME NOT NULL DEFAULT NOW() ON UPDATE NOW(),
  UNIQUE KEY `unique_file_sha1` (`file_sha1`),
  KEY `fingerprinted` (`fingerprinted`),
  KEY `song_name` (`song_name`),
  KEY `artist` (`artist`)
) ENGINE=INNODB;

-- Fingerprints table
CREATE TABLE IF NOT EXISTS fingerprints (
  hash CHAR(10) NOT NULL,
  song_id INT NOT NULL,
  offset INT NOT NULL,
  INDEX (hash),
  UNIQUE KEY `unique_constraint` (`song_id`, `offset`, `hash`),
  FOREIGN KEY (song_id) REFERENCES songs(song_id) ON DELETE CASCADE
) ENGINE=INNODB;

-- Optimizations
CREATE INDEX hash_index ON fingerprints (hash);
CREATE INDEX song_id_index ON fingerprints (song_id);

-- Statistics table for monitoring
CREATE TABLE IF NOT EXISTS scraper_stats (
  id INT AUTO_INCREMENT PRIMARY KEY,
  station_name VARCHAR(100) NOT NULL,
  date DATE NOT NULL,
  hour INT NOT NULL,
  captures INT DEFAULT 0,
  detections INT DEFAULT 0,
  avg_confidence DECIMAL(5,4) DEFAULT 0,
  UNIQUE KEY unique_stat (station_name, date, hour)
) ENGINE=INNODB;
