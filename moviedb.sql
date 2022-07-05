create table file(
path varchar not null,
name varchar not null,
file_size int not null,
mtime int not null,
time timestamp not null default current_timestamp,
primary key(path, name)
);

create table video_stream (
path varchar not null,
file varchar not null,
position integer not null,
codec varchar, 
aspect varchar, 
width int, 
height int, 
duration float,
fps float,
primary key(path, file, position),
foreign key(path, file) references file(path, name) on update cascade on delete cascade
);

create table audio_stream (
path varchar not null,
file varchar not null,
position integer not null, 
codec varchar, 
language varchar, 
channels int,
primary key(path, file, position),
foreign key(path, file) references file(path, name) on update cascade on delete cascade
);

create table subtitle_stream (
path varchar not null,
file varchar not null,
position integer not null, 
language varchar,
primary key(path, file, position),
foreign key(path, file) references file(path, name) on update cascade on delete cascade
);

create table thumb(
path varchar not null,
file varchar not null,
stream integer not null, 
position integer not null,
data blob not null,
primary key(path, file, stream, position),
foreign key(path, file, stream) references video_stream(path, file, position) on update cascade on delete cascade
);