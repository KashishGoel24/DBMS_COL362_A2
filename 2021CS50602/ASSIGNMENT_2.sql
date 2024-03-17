-- create tables

create table department (
    dept_id char(3) not null,
    dept_name varchar(40) not null,
    unique(dept_name),
    primary key(dept_id)
);


create table valid_entry (
    dept_id char(3),
    entry_year integer not null,
    seq_number integer not null,
    foreign key(dept_id) references department(dept_id) ON UPDATE CASCADE ON DELETE CASCADE
);


create table professor (
    professor_id varchar(10) not null,
    professor_first_name varchar(40) not null,
    professor_last_name varchar(40) not null,
    office_number varchar(20),
    contact_number char(10) not null,
    start_year integer,
    resign_year integer,
    dept_id char(3),
    primary key (professor_id),
    foreign key (dept_id) references department(dept_id) ON UPDATE CASCADE ON DELETE CASCADE,
    check (start_year <= resign_year)
);


CREATE OR REPLACE FUNCTION validate_course_id(course_id CHAR(6),dept_id char(3)) RETURNS BOOLEAN AS 
$$
BEGIN 
    RETURN (substring(course_id, 1, 3) = dept_id AND substring(course_id, 4, 3) ~ '^[0-9]+$');
END;
$$ LANGUAGE plpgsql;

CREATE TABLE courses (
    course_id CHAR(6) NOT NULL CHECK (validate_course_id(course_id,dept_id)),
    course_name VARCHAR(20) NOT NULL,
    course_desc TEXT,
    credits NUMERIC NOT NULL,
    dept_id CHAR(3),
    UNIQUE(course_name),
    PRIMARY KEY(course_id),
    FOREIGN KEY(dept_id) REFERENCES department(dept_id) ON UPDATE CASCADE ON DELETE CASCADE,
    CHECK(credits > 0)
);


create table course_offers (
    course_id char(6),
    session varchar(9),
    semester integer not null check((semester = 1 or semester = 2)),
    professor_id varchar(10),
    capacity integer,
    enrollments integer,
    primary key (course_id, session, semester),
    foreign key (course_id) references courses(course_id) ON UPDATE CASCADE ON DELETE CASCADE,
    foreign key(professor_id) references professor(professor_id) ON DELETE CASCADE
);



create table student (
    first_name varchar(40) not null,
    last_name varchar(40),
    student_id char(11) not null CHECK (length(student_id) = 10),
    address varchar(100),
    contact_number char(10) not null,
    email_id varchar(50),
    tot_credits numeric not null,
    dept_id char(3),
    unique(contact_number, email_id),
    primary key (student_id),
    foreign key (dept_id) references department (dept_id) ON UPDATE CASCADE,
    check(tot_credits >= 0)
);


create table student_courses (
    student_id char(11),
    course_id char(6),
    session varchar(9),
    semester integer check((semester = 1 or semester = 2)),
    grade numeric not null check (0 <= grade and grade <= 10),
    foreign key(student_id) references student(student_id) ON UPDATE CASCADE ON DELETE CASCADE ,
    foreign key (course_id, session, semester ) references course_offers(course_id, session, semester) ON DELETE CASCADE ON UPDATE CASCADE,
    primary key (student_id, course_id, session, semester)
);


-- student table triggers to ensure correct student id and email id

create or replace function validate_student_id()
returns trigger as 
$$
    declare 
        entry_year1 int;
        dept_id1 char(3);
        seq_number1 int;
    begin
        entry_year1 := substring(NEW.student_id from 1 for 4)::INT;
        dept_id1 := substring(NEW.student_id from 5 for 3);
        seq_number1 := substring(NEW.student_id from 8 for 3)::INT;

    IF NEW.dept_id <> dept_id1 THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM valid_entry 
        WHERE valid_entry.entry_year = entry_year1 AND valid_entry.dept_id = dept_id1
        AND valid_entry.seq_number = seq_number1
    ) THEN 
        RAISE EXCEPTION 'invalid';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

create trigger validate_student_id BEFORE INSERT ON student for each row
    execute function validate_student_id();

create or replace function update_seq_number() returns trigger as 
$$
declare 
    entry_year1 integer;
begin
    entry_year1 := substring(NEW.student_id from 1 for 4)::INT;

    update valid_entry
    set seq_number = seq_number + 1
    where valid_entry.dept_id = NEW.dept_id AND valid_entry.entry_year = entry_year1;

    return NEW;
END;
$$ LANGUAGE plpgsql;

create trigger update_seq_number after insert on student 
for each row
execute function update_seq_number();

create or replace function validate_student_email()
returns trigger as
$$
declare 
    dept_id1 char(3);
    entry_year CHAR(4);
    dept_id CHAR(3);
    seq_number CHAR(3);
    email_prefix VARCHAR(12);
begin
    dept_id1 := substring(NEW.email_id from 12 for 3);
    entry_year := substring(NEW.student_id from 1 for 4);
    dept_id := substring(NEW.student_id from 5 for 3);
    seq_number := substring(NEW.student_id from 8 for 3);
    email_prefix := entry_year || dept_id || seq_number;

    IF NEW.dept_id <> dept_id1 THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    IF NEW.email_id = email_prefix || '@' || dept_id1 || '.iitd.ac.in' THEN
        RETURN NEW;
    ELSE
        RAISE EXCEPTION 'invalid';
    END IF;
END;
$$ LANGUAGE plpgsql;

create trigger validate_student_email before insert on student 
    for each row execute function validate_student_email();


-- department change 

create table student_dept_change(
    old_student_id char(11),
    old_dept_id char(3),
    new_dept_id char(3),
    new_student_id char(11),
    foreign key(old_dept_id) references department(dept_id) ON UPDATE CASCADE ON DELETE CASCADE,
    foreign key(new_dept_id) references department(dept_id) ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE OR REPLACE FUNCTION log_student_dept_change()
RETURNS TRIGGER AS
$$
DECLARE
    entry_year1 integer;
    avg_grade float;
BEGIN
    entry_year1 := substring(NEW.student_id FROM 1 FOR 4)::INT;

    IF NEW.dept_id <> OLD.dept_id THEN

        IF EXISTS (
            SELECT 1
            FROM student_dept_change
            WHERE student_dept_change.new_student_id = OLD.student_id
        ) THEN
            RAISE EXCEPTION 'Department can be changed only once';
        END IF;

        IF entry_year1 < 2022 THEN
            RAISE EXCEPTION 'Entry year must be >=2022';
        END IF;

        SELECT AVG(grade) INTO avg_grade FROM student_courses WHERE student_id = OLD.student_id;

        IF avg_grade IS NULL OR avg_grade <= 8.5 THEN
            RAISE EXCEPTION 'Low Grade';
        END IF;

    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

create trigger log_student_dept_change before UPDATE ON student FOR EACH ROW
WHEN (pg_trigger_depth() < 1) 
EXECUTE FUNCTION log_student_dept_change();

CREATE OR REPLACE FUNCTION log_student_dept_change_after()
RETURNS TRIGGER AS
$$
DECLARE
    new_student_id CHAR(11);
    new_seq_number integer;
    new_email_id varchar(50);
    entry_year1 integer;
BEGIN
    entry_year1 := substring(NEW.student_id from 1 for 4)::INT;
    IF NEW.dept_id <> OLD.dept_id THEN

        SELECT seq_number INTO new_seq_number FROM valid_entry WHERE dept_id = NEW.dept_id AND entry_year = entry_year1;

        new_student_id := entry_year1 || NEW.dept_id || LPAD(new_seq_number::TEXT, 3, '0');
        new_email_id := new_student_id || '@' || NEW.dept_id || '.iitd.ac.in';

        INSERT INTO student_dept_change (old_student_id, old_dept_id, new_dept_id, new_student_id) VALUES (OLD.student_id, OLD.dept_id, NEW.dept_id, new_student_id);

        UPDATE valid_entry SET seq_number = seq_number + 1 WHERE dept_id = NEW.dept_id AND entry_year = entry_year1;

        UPDATE student
        SET
            student_id = new_student_id,
            email_id = new_email_id
        WHERE student_id = OLD.student_id;

    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER log_student_dept_change_after
AFTER UPDATE ON student
FOR EACH ROW
WHEN (pg_trigger_depth() < 1)
EXECUTE FUNCTION log_student_dept_change_after();


-- course_eval view

CREATE MATERIALIZED VIEW course_eval AS
SELECT
    sc.course_id,
    sc.session,
    sc.semester,
    COUNT(sc.student_id) AS number_of_students,
    AVG(sc.grade) AS average_grade,
    MAX(sc.grade) AS max_grade,
    MIN(sc.grade) AS min_grade
FROM
    student_courses sc
GROUP BY
    sc.course_id, sc.session, sc.semester;

CREATE OR REPLACE FUNCTION refresh_course_eval_trigger()
RETURNS TRIGGER AS
$$
BEGIN
    REFRESH MATERIALIZED VIEW course_eval;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER refresh_course_eval_trigger
AFTER INSERT OR UPDATE OR DELETE
ON student_courses
FOR EACH STATEMENT
EXECUTE FUNCTION refresh_course_eval_trigger();


-- student courses table

-- update enrollments and total credits after insert

CREATE OR REPLACE FUNCTION update_tot_credits_enrollment()
RETURNS TRIGGER AS 
$$
DECLARE
    course_credits INTEGER;
BEGIN
    SELECT credits INTO course_credits
    FROM courses
    WHERE course_id = NEW.course_id;

    UPDATE student
    SET tot_credits = tot_credits + course_credits
    WHERE student_id = NEW.student_id;

    UPDATE course_offers
    SET enrollments = enrollments + 1 
    WHERE course_id = NEW.course_id AND session = NEW.session AND semester = NEW.semester;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_tot_credits_and_enrollment 
AFTER INSERT ON student_courses
FOR EACH ROW EXECUTE FUNCTION update_tot_credits_enrollment();

-- Update enrollments and total credits after delete

CREATE OR REPLACE FUNCTION update_tot_credits_enrollment2()
RETURNS TRIGGER AS 
$$
DECLARE 
    total_credits integer;
    total_enrollments integer;
BEGIN
    SELECT COALESCE(SUM(c.credits), 0) INTO total_credits 
    FROM courses c
    JOIN student_courses sc ON c.course_id = sc.course_id
    WHERE sc.student_id = OLD.student_id;

    UPDATE student
        SET tot_credits = total_credits
        WHERE student_id = OLD.student_id;

    SELECT COUNT(student_id) INTO total_enrollments 
    FROM course_offers c
    JOIN student_courses sc ON c.course_id = sc.course_id and c.session = sc.session and c.semester = sc.semester;

    UPDATE course_offers
    SET enrollments = total_enrollments 
    WHERE course_id = OLD.course_id and session = OLD.session and semester = OLD.semester;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_tot_credits_and_enrollment2 
AFTER DELETE ON student_courses
FOR EACH ROW EXECUTE FUNCTION update_tot_credits_enrollment2();

-- student_courses continued

CREATE OR REPLACE FUNCTION check_course_and_credit_limit() RETURNS TRIGGER AS 
$$
DECLARE
    total_courses INTEGER;
    total_credits INTEGER;
    course_credits integer;
    semester_credit_count integer;
BEGIN
    SELECT COUNT(*) INTO total_courses
    FROM student_courses
    WHERE student_id = NEW.student_id
      AND session = NEW.session
      AND semester = NEW.semester;

    IF total_courses >= 5 THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    SELECT tot_credits INTO total_credits
    FROM student WHERE student_id = NEW.student_id;

    SELECT credits INTO course_credits
    FROM courses
    WHERE course_id = NEW.course_id;
    
    SELECT COALESCE(SUM(c.credits), 0) INTO semester_credit_count 
    FROM courses c
    JOIN student_courses sc ON c.course_id = sc.course_id
    WHERE sc.session = NEW.session AND sc.semester = NEW.semester AND sc.student_id = NEW.student_id;

    IF semester_credit_count + course_credits > 26 THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    IF total_credits + course_credits > 60 THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER check_course_and_credit_limit
BEFORE INSERT ON student_courses
FOR EACH ROW
EXECUTE FUNCTION check_course_and_credit_limit();


CREATE OR REPLACE FUNCTION check_five_credit_course() RETURNS TRIGGER AS 
$$
DECLARE
    entry_year_stu INTEGER;
    session_year INTEGER;
    credit_count INTEGER;
BEGIN
    entry_year_stu := SUBSTRING(NEW.student_id FROM 1 FOR 4)::INT;
    session_year := SUBSTRING(NEW.session FROM 1 FOR 4)::INT;

    SELECT credits INTO credit_count
    FROM courses WHERE course_id = NEW.course_id;

    IF credit_count = 5 THEN
        IF session_year <> entry_year_stu THEN
            RAISE EXCEPTION 'invalid';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_five_credit_course
BEFORE INSERT ON student_courses
FOR EACH ROW
EXECUTE FUNCTION check_five_credit_course();


CREATE MATERIALIZED VIEW student_semester_summary AS
SELECT
    sc.student_id,
    sc.session,
    sc.semester,
    SUM(sc.grade * c.credits)/SUM(c.credits) FILTER (WHERE sc.grade >= 5.0 and sc.course_id = c.course_id) AS sgpa,
    SUM(c.credits) FILTER (WHERE sc.grade >= 5.0) AS credits
FROM
    student_courses sc
JOIN
    courses c ON sc.course_id = c.course_id
GROUP BY
    sc.student_id, sc.session, sc.semester;

CREATE OR REPLACE FUNCTION update_student_summary()
RETURNS TRIGGER AS
$$
BEGIN
    REFRESH MATERIALIZED VIEW student_semester_summary;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_student_summary_trigger
AFTER INSERT OR UPDATE OR DELETE
ON student_courses
FOR EACH ROW
EXECUTE FUNCTION update_student_summary();


CREATE OR REPLACE FUNCTION check_course_capacity() RETURNS TRIGGER AS 
$$
DECLARE
    course_capacity INTEGER;
    current_capacity integer;
BEGIN
    SELECT capacity, enrollments INTO course_capacity, current_capacity
    FROM course_offers
    WHERE course_id = NEW.course_id
        AND session = NEW.session
        AND semester = NEW.semester;

    IF course_capacity = current_capacity THEN
        RAISE EXCEPTION 'course is full';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_course_capacity
BEFORE INSERT ON student_courses
FOR EACH ROW
EXECUTE FUNCTION check_course_capacity();


-- course_offers table

CREATE OR REPLACE FUNCTION check_course_offers()
RETURNS TRIGGER AS 
$$
DECLARE
    courses_count INTEGER;
    teaching_year integer;
    prof_resign_year integer;
BEGIN
    teaching_year := SUBSTRING(NEW.session FROM 1 FOR 4)::INT;

    SELECT COUNT(*) INTO courses_count
    FROM course_offers
    WHERE professor_id = NEW.professor_id
        AND session = NEW.session;

    select resign_year into prof_resign_year 
    from professor where professor_id = NEW.professor_id;

    IF courses_count >= 4 THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    IF prof_resign_year <= teaching_year THEN
        RAISE EXCEPTION 'invalid';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_course_offers BEFORE INSERT
ON course_offers FOR EACH ROW
EXECUTE FUNCTION check_course_offers();

-- department id update in dept table

CREATE OR REPLACE FUNCTION generate_new_student_id(old_student_id CHAR(11), new_dept_id CHAR(3))
RETURNS CHAR(11) AS
$$
DECLARE
    entry_year INT;
    seq_number INT;
    new_student_id CHAR(11);
BEGIN
    entry_year := CAST(SUBSTRING(old_student_id FROM 1 FOR 4) AS INT);
    seq_number := CAST(SUBSTRING(old_student_id FROM 8 FOR 3) AS INT);

    new_student_id := entry_year || new_dept_id || LPAD(seq_number::TEXT, 3, '0');

    RETURN new_student_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION generate_new_email_id(old_student_id CHAR(11), new_dept_id CHAR(3))
RETURNS CHAR(11) AS
$$
DECLARE
    entry_year INT;
    seq_number INT;
    new_student_id CHAR(11);
    new_email_id varchar(50);
BEGIN
    entry_year := CAST(SUBSTRING(old_student_id FROM 1 FOR 4) AS INT);
    seq_number := CAST(SUBSTRING(old_student_id FROM 8 FOR 3) AS INT);

    new_student_id := entry_year || new_dept_id || LPAD(seq_number::TEXT, 3, '0');
    new_email_id := new_student_id ||  '@' || new_dept_id || '.iitd.ac.in';

    RETURN new_email_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_department_trigger_function()
RETURNS TRIGGER AS
$$
DECLARE
    new_student_id CHAR(11);
BEGIN
    IF NEW.dept_id <> OLD.dept_id THEN
    
        UPDATE student
            SET student_id = generate_new_student_id(student_id, NEW.dept_id),
                email_id = generate_new_email_id(student_id, NEW.dept_id)
            WHERE dept_id = OLD.dept_id;
            
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_department_trigger
BEFORE UPDATE ON department
FOR EACH ROW
EXECUTE FUNCTION update_department_trigger_function();

CREATE OR REPLACE FUNCTION update_course_id_dept_trigger()
RETURNS TRIGGER AS
$$
BEGIN
    IF NEW.dept_id <> OLD.dept_id THEN
    
        NEW.course_id := NEW.dept_id || SUBSTRING(OLD.course_id FROM 4 for 3);
            
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER update_department_trigger2
BEFORE UPDATE ON courses
FOR EACH ROW
EXECUTE FUNCTION update_course_id_dept_trigger();


-- delete from department trigger

CREATE OR REPLACE FUNCTION delete_department_trigger_function()
RETURNS TRIGGER AS
$$
BEGIN
    IF EXISTS (SELECT 1 FROM student WHERE dept_id = OLD.dept_id) THEN
        RAISE EXCEPTION 'Department has students';
    END IF;

    RETURN OLD;
END; 
$$ LANGUAGE plpgsql;

CREATE TRIGGER delete_department_trigger
BEFORE DELETE ON department
FOR EACH ROW
EXECUTE FUNCTION delete_department_trigger_function();
