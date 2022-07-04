import os, sys, argparse, subprocess, json, tempfile, io, re, csv, datetime
import sqlite3
from PIL import Image

if sys.platform == "win32":
    os.environ["PATH"] = os.environ["PATH"] + ";" + os.path.dirname(os.path.realpath(__file__))

def walklevel(some_dir, level=1):
    some_dir = some_dir.rstrip(os.path.sep)
    assert os.path.isdir(some_dir)
    num_sep = some_dir.count(os.path.sep)
    for root, dirs, files in os.walk(some_dir):
        yield root, dirs, files
        num_sep_this = root.count(os.path.sep)
        if num_sep + level <= num_sep_this:
            del dirs[:]

def export( conn, path ) :
    try :
        with open(path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=';', quotechar='|', quoting=csv.QUOTE_MINIMAL)
            writer.writerow( ["Директория", "Имя", "Размер", "Длительность", "Количество видеопотоков", "Количество аудиопотоков", "Количество субтитров", "Кодек"] )
            cursor = conn.cursor()
            cursor.execute( "SELECT path, name, file_size, (SELECT count(*) FROM video_stream WHERE video_stream.path=path AND video_stream.file=name), (SELECT count(*) FROM audio_stream WHERE audio_stream.path=path AND audio_stream.file=name), (SELECT count(*) FROM subtitle_stream WHERE subtitle_stream.path=path AND subtitle_stream.file=name) FROM file ORDER BY path, name" )
            for path, name, file_size, videos, audios, subtitles in cursor.fetchall() :
                cursor.execute( "SELECT duration, codec, fps FROM video_stream WHERE path=? AND file=? LIMIT 1", (path, name) )
                duration, codec, fps = cursor.fetchone()
                writer.writerow( [path, name, file_size, duration, videos, audios, subtitles, codec] )
    except Exception :
        print("Cannot write file '{}'".format(path))

def clear(conn) :
    cursor = conn.cursor()
    cursor.execute( "SELECT path, name FROM file" )
    for path, name in cursor.fetchall() :
        if not os.path.exists( os.path.join( path, name ) ) :
            print( "Remove '{}' FROM '{}' directory".format( name, path ) )
            cursor.execute( "DELETE FROM thumb WHERE path=? AND file=?", (path, name) )
            cursor.execute( "DELETE FROM video_stream WHERE path=? AND file=?", (path, name) )
            cursor.execute( "DELETE FROM audio_stream WHERE path=? AND file=?", (path, name) )
            cursor.execute( "DELETE FROM subtitle_stream WHERE path=? AND file=?", (path, name) )
            cursor.execute( "DELETE FROM file WHERE path=? AND name=?", (path, name) )
            conn.commit()

def scan(conn, base_path, *, depth=10, types=[".mp4", ".m4v", ".mpg", ".mkv", ".avi"] ) :
    if not os.path.exists( base_path ) :
        print( "No such directory" )
        return
    for path, dirs, files in walklevel( base_path, depth ) :
        for file in files :
            if os.path.splitext(file)[1].lower() in types :
                print(file)
                filepath = os.path.join( path, file )
                stat = os.lstat(filepath)
                cursor = conn.cursor()
                cursor.execute( "SELECT * FROM file WHERE name=? AND path=?", (file, path) )
                if cursor.fetchone() :
                    print("File exists. Update file info? Y/N:")
                    answer = input()
                    if answer.strip().lower() not in ["y", "yes"] :
                        continue
                    else :
                        cursor.execute( "DELETE FROM thumb WHERE path=? AND file=?", (path, file) )
                        cursor.execute( "DELETE FROM video_stream WHERE path=? AND file=?", (path, file) )
                        cursor.execute( "DELETE FROM audio_stream WHERE path=? AND file=?", (path, file) )
                        cursor.execute( "DELETE FROM subtitle_stream WHERE path=? AND file=?", (path, file) )
                        cursor.execute( "DELETE FROM file WHERE path=? AND name=?", (path, file) )
                cursor.execute( "INSERT INTO file(name, path, file_size, mtime, time) VALUES(?, ?, ?, ?, ?)", ( file, path, stat.st_size, stat.st_mtime, datetime.datetime.now() ) )
                p = subprocess.Popen( 'ffprobe -v quiet -print_format json -show_streams "{}"'.format( filepath ), shell=True, stdout=subprocess.PIPE )
                output = p.stdout.read()
                info = json.loads( output )
                #print(info)
                #input()
                if 'streams' in info and len( info['streams'] ) > 0 :
                    video_stream_counter = 0
                    audio_stream_counter = 0
                    subtitle_stream_counter = 0
                    for stream in info['streams'] :
                        if stream["codec_type"] == "video" :                            
                            codec = stream["codec_name"]
                            aspect = "{:.6f}".format( float( stream["width"] ) / stream["height"])
                            width = stream["width"]
                            height = stream["height"]
                            fps = int(re.match( r'(?P<fps>\d)+/\d', stream["avg_frame_rate"] ).group("fps"))

                            thumbs = []
                            if "duration" in stream :
                                duration = round(float(stream["duration"]))
                            else :
                                p = subprocess.Popen( 'ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "{}"'.format( filepath ), shell=True, stdout=subprocess.PIPE )
                                duration = float( p.stdout.read().strip() )
                            for i in [0.1, 0.5, 0.9] :
                                mark = int(i*duration)
                                temp_name = os.path.join( tempfile._get_default_tempdir(), next(tempfile._get_candidate_names()) + '.jpg' )
                                subprocess.call( 'ffmpeg -v error -ss {} -i "{}" -vframes 1 -q:v 2 "{}"'.format( mark, filepath, temp_name), shell=True )
                                thumb = io.BytesIO()
                                with Image.open(temp_name) as im :
                                    im.thumbnail((512,512))
                                    im.save( thumb, "PNG" )
                                    thumb.seek(0)
                                thumbs.append( (mark, thumb) )
                            cursor.execute( "INSERT INTO video_stream(path, file, position, codec, aspect, width, height, duration, fps) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)", (path, file, video_stream_counter, codec, aspect, width, height, duration, fps) )
                            for mark, thumb in thumbs :
                                cursor.execute( "INSERT INTO thumb(path, file, stream, position, data) VALUES(?, ?, ?, ?, ?)", (path, file, video_stream_counter, mark, thumb.read()) )
                            video_stream_counter += 1
                        elif stream["codec_type"] == "audio" :
                            codec = stream["codec_name"]
                            if "tags" in stream and "language" in stream["tags"] :
                                language = stream["tags"]["language"]
                            else :
                                language = None                            
                            channels = stream["channels"]
                            cursor.execute( "INSERT INTO audio_stream(path, file, position, codec, language, channels) VALUES(?, ?, ?, ?, ?, ?)", (path, file, audio_stream_counter, codec, language, channels) )
                            audio_stream_counter += 1
                        elif stream["codec_type"] == "subtitle" :
                            if "tags" in stream and "language" in stream["tags"] :
                                language = stream["tags"]["language"]
                            else :
                                language = None
                            cursor.execute( "INSERT INTO subtitle_stream(path, file, position, language) VALUES(?, ?, ?, ?)", (path, file, subtitle_stream_counter, language) )
                            subtitle_stream_counter += 1
                else :
                    print("{} has no streams".format( os.path.join(path, file) ))
                conn.commit()
                cursor.close()


if __name__ == "__main__" :
    parser = argparse.ArgumentParser(description='moviedb')
    parser.add_argument('--path', dest='path', default=None, action='store', help='Path to scanning directory') # сканирование директории
    parser.add_argument('--export', dest='export', default=None, action='store', help='Export database as CSV file') # экспорт базы данных как файла с расширением csv
    parser.add_argument('--database', dest='database', default='moviedb.sqlite3', action='store', help='Path to database') # путь к базе данных
    parser.add_argument('--clear', dest='clear', action='store_const', const=True, help='Remove all unexists files from database') # ужалить все несуществующие файлы из базы данных
    parser.add_argument('--files', dest='files', default="mp4,m4v,mpg,mkv,avi", action='store', help='Scanning files types') # типы (расширения) сканируемых файлов
    parser.add_argument('--depth', dest='depth', default=10, type=int, action='store', help='Scanning directory depth') # указать глубину сканирования директории

    args = vars( parser.parse_args( sys.argv[1:] ) )

    if not os.path.exists( args["database"] ) :
        with open( os.path.join(os.path.dirname(os.path.realpath(__file__)), "moviedb.sql") ) as p :
            script = p.read().split( ";" )
        conn = sqlite3.connect(args["database"])
        [ conn.cursor().execute( i ) for i in script ]
        conn.commit()
    else :
        conn = sqlite3.connect(args["database"])

    if args["path"] :
        types = [ "." + ext.strip() for ext in args["files"].split(",") ]
        scan( conn, os.path.abspath(args["path"]), depth=args["depth"], types=types )
    if args["clear"] :
        clear( conn )
    if args["export"] :
        export( conn, args["export"] )