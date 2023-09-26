
-- CREATE DATABASE "adventure_mmo_game";
GRANT ALL PRIVILEGES ON DATABASE "adventure_mmo_game" TO postgres;

CREATE TABLE "public"."user_info" (
    user_id integer NOT NULL,
    birthday date,
    sign_in_date date,
    sex text,
    country text
);



-- ALTER TABLE public.user_info OWNER TO postgres;


INSERT INTO "public"."user_info" VALUES (1, '2001-11-04', '2023-04-07', 'Male', 'Thailand');
INSERT INTO "public"."user_info" VALUES (2, '2001-08-10', '2021-09-27', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (3, '2003-07-18', '2022-08-07', 'Male', 'Lao');
INSERT INTO public.user_info VALUES (4, '1997-05-17', '2023-06-12', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (5, '1997-07-24', '2023-04-04', 'Female', 'Lao');
INSERT INTO public.user_info VALUES (6, '2004-03-28', '2022-12-10', 'Male', 'Vietnam');
INSERT INTO public.user_info VALUES (7, '2004-09-12', '2022-11-06', 'Male', 'Vietnam');
INSERT INTO public.user_info VALUES (8, '1994-03-18', '2022-03-31', 'Male', 'Singapore');
INSERT INTO public.user_info VALUES (9, '2001-10-12', '2021-11-17', 'Male', 'Lao');
INSERT INTO public.user_info VALUES (10, '2000-11-01', '2023-04-22', 'Male', 'Lao');
INSERT INTO public.user_info VALUES (11, '2004-07-31', '2022-02-24', 'Male', 'Singapore');
INSERT INTO public.user_info VALUES (12, '1997-01-23', '2022-06-12', 'Male', 'Singapore');
INSERT INTO public.user_info VALUES (13, '2000-08-19', '2022-07-05', 'Female', 'Vietnam');
INSERT INTO public.user_info VALUES (14, '1997-01-06', '2023-06-05', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (15, '1998-09-03', '2022-01-03', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (16, '2003-01-27', '2023-06-21', 'Male', 'Lao');
INSERT INTO public.user_info VALUES (17, '1999-06-13', '2022-06-21', 'Male', 'Singapore');
INSERT INTO public.user_info VALUES (18, '1995-12-02', '2022-11-19', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (19, '2004-01-01', '2023-03-21', 'Male', 'Singapore');
INSERT INTO public.user_info VALUES (20, '2000-07-09', '2023-06-04', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (21, '1996-06-14', '2022-05-04', 'Male', 'Vietnam');
INSERT INTO public.user_info VALUES (22, '1994-08-01', '2022-01-12', 'Female', 'Vietnam');
INSERT INTO public.user_info VALUES (23, '2000-05-06', '2022-08-01', 'Male', 'Lao');
INSERT INTO public.user_info VALUES (24, '1997-10-08', '2023-04-05', 'Male', 'Vietnam');
INSERT INTO public.user_info VALUES (25, '2000-08-10', '2022-05-10', 'Male', 'Vietnam');
INSERT INTO public.user_info VALUES (26, '1993-11-22', '2023-04-13', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (27, '2000-01-01', '2022-08-24', 'Male', 'Vietnam');
INSERT INTO public.user_info VALUES (28, '2002-01-28', '2022-10-05', 'Female', 'Lao');
INSERT INTO public.user_info VALUES (29, '2000-01-17', '2022-04-21', 'Female', 'Singapore');
INSERT INTO public.user_info VALUES (30, '1995-05-04', '2023-03-14', 'Male', 'Singapore');
INSERT INTO public.user_info VALUES (31, '1998-02-02', '2022-12-03', 'Male', 'Singapore');
INSERT INTO public.user_info VALUES (32, '1997-12-29', '2023-06-21', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (33, '1994-10-09', '2023-07-08', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (34, '1993-11-12', '2022-08-27', 'Male', 'Singapore');
INSERT INTO public.user_info VALUES (35, '2004-03-17', '2023-06-26', 'Female', 'Thailand');
INSERT INTO public.user_info VALUES (36, '2001-10-15', '2021-12-27', 'Male', 'Vietnam');
INSERT INTO public.user_info VALUES (37, '2002-06-14', '2022-08-25', 'Male', 'Singapore');
INSERT INTO public.user_info VALUES (38, '1997-02-03', '2021-12-14', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (39, '1994-08-23', '2022-03-22', 'Female', 'Singapore');
INSERT INTO public.user_info VALUES (40, '1992-09-11', '2022-07-25', 'Male', 'Vietnam');
INSERT INTO public.user_info VALUES (41, '2002-03-14', '2022-07-19', 'Female', 'Thailand');
INSERT INTO public.user_info VALUES (42, '2005-03-28', '2023-08-01', 'Male', 'Vietnam');
INSERT INTO public.user_info VALUES (43, '1996-04-16', '2023-02-11', 'Female', 'Lao');
INSERT INTO public.user_info VALUES (44, '2000-08-17', '2022-04-18', 'Female', 'Vietnam');
INSERT INTO public.user_info VALUES (45, '1994-01-22', '2021-10-11', 'Male', 'Singapore');
INSERT INTO public.user_info VALUES (46, '2001-06-28', '2022-02-04', 'Male', 'Vietnam');
INSERT INTO public.user_info VALUES (47, '2004-05-02', '2023-06-21', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (48, '1992-10-14', '2023-04-18', 'Male', 'Lao');
INSERT INTO public.user_info VALUES (49, '2004-09-17', '2022-09-04', 'Male', 'Vietnam');
INSERT INTO public.user_info VALUES (50, '1995-02-22', '2021-09-09', 'Male', 'Lao');
INSERT INTO public.user_info VALUES (51, '2000-09-26', '2022-03-01', 'Female', 'Thailand');
INSERT INTO public.user_info VALUES (52, '1996-09-19', '2022-01-03', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (53, '1998-09-06', '2023-03-28', 'Female', 'Vietnam');
INSERT INTO public.user_info VALUES (54, '1994-02-04', '2023-01-31', 'Male', 'Singapore');
INSERT INTO public.user_info VALUES (55, '2004-06-03', '2023-04-21', 'Male', 'Lao');
INSERT INTO public.user_info VALUES (56, '1993-10-20', '2022-05-02', 'Male', 'Vietnam');
INSERT INTO public.user_info VALUES (57, '1993-09-19', '2021-09-15', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (58, '2005-05-31', '2022-03-22', 'Male', 'Vietnam');
INSERT INTO public.user_info VALUES (59, '1992-08-17', '2022-06-06', 'Female', 'Vietnam');
INSERT INTO public.user_info VALUES (60, '2000-05-26', '2022-06-03', 'Male', 'Lao');
INSERT INTO public.user_info VALUES (61, '1999-10-20', '2022-04-14', 'Male', 'Lao');
INSERT INTO public.user_info VALUES (62, '2004-07-29', '2023-07-25', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (63, '1996-01-16', '2022-07-18', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (64, '2004-08-13', '2021-09-01', 'Female', 'Lao');
INSERT INTO public.user_info VALUES (65, '1999-05-16', '2022-07-02', 'Male', 'Vietnam');
INSERT INTO public.user_info VALUES (66, '1997-09-22', '2022-01-07', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (67, '2004-07-12', '2021-11-01', 'Male', 'Lao');
INSERT INTO public.user_info VALUES (68, '2001-07-08', '2022-04-09', 'Male', 'Lao');
INSERT INTO public.user_info VALUES (69, '2004-08-09', '2022-07-16', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (70, '1999-10-14', '2023-07-06', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (71, '1999-12-23', '2022-11-06', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (72, '1995-10-24', '2023-05-02', 'Male', 'Vietnam');
INSERT INTO public.user_info VALUES (73, '2003-09-23', '2022-04-20', 'Male', 'Singapore');
INSERT INTO public.user_info VALUES (74, '1992-09-04', '2021-12-19', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (75, '2001-06-13', '2021-10-27', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (76, '1993-04-07', '2022-08-13', 'Male', 'Lao');
INSERT INTO public.user_info VALUES (77, '1997-01-23', '2021-08-31', 'Female', 'Vietnam');
INSERT INTO public.user_info VALUES (78, '2001-03-27', '2022-05-30', 'Male', 'Singapore');
INSERT INTO public.user_info VALUES (79, '1998-01-13', '2022-12-18', 'Male', 'Singapore');
INSERT INTO public.user_info VALUES (80, '2004-08-15', '2021-11-12', 'Male', 'Singapore');
INSERT INTO public.user_info VALUES (81, '2005-01-07', '2022-09-05', 'Male', 'Singapore');
INSERT INTO public.user_info VALUES (82, '1993-04-03', '2022-05-22', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (83, '1995-04-14', '2022-01-11', 'Male', 'Singapore');
INSERT INTO public.user_info VALUES (84, '1992-12-14', '2022-04-06', 'Male', 'Vietnam');
INSERT INTO public.user_info VALUES (85, '1993-07-17', '2022-11-12', 'Male', 'Singapore');
INSERT INTO public.user_info VALUES (86, '2002-08-29', '2023-02-21', 'Female', 'Lao');
INSERT INTO public.user_info VALUES (87, '2004-02-07', '2022-04-04', 'Male', 'Vietnam');
INSERT INTO public.user_info VALUES (88, '1998-10-12', '2021-09-13', 'Female', 'Singapore');
INSERT INTO public.user_info VALUES (89, '1997-03-01', '2022-09-30', 'Male', 'Vietnam');
INSERT INTO public.user_info VALUES (90, '1994-09-20', '2022-09-01', 'Male', 'Singapore');
INSERT INTO public.user_info VALUES (91, '1994-12-22', '2023-02-09', 'Male', 'Vietnam');
INSERT INTO public.user_info VALUES (92, '1998-04-22', '2023-06-05', 'Female', 'Vietnam');
INSERT INTO public.user_info VALUES (93, '2002-03-13', '2022-11-07', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (94, '2003-02-18', '2021-09-25', 'Male', 'Singapore');
INSERT INTO public.user_info VALUES (95, '1993-07-03', '2022-04-17', 'Male', 'Vietnam');
INSERT INTO public.user_info VALUES (96, '2001-09-06', '2021-09-15', 'Male', 'Vietnam');
INSERT INTO public.user_info VALUES (97, '2000-11-25', '2023-07-23', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (98, '1995-05-13', '2022-03-25', 'Female', 'Lao');
INSERT INTO public.user_info VALUES (99, '1992-12-13', '2022-12-05', 'Male', 'Thailand');
INSERT INTO public.user_info VALUES (100, '2005-01-13', '2023-01-28', 'Male', 'Singapore');


ALTER TABLE ONLY public.user_info
ADD CONSTRAINT user_info_pkey PRIMARY KEY (user_id);
