----------------------------------------------------------------------------------------
---extra data
-- Insert new patients for Region 1
INSERT INTO region1_raw.patients (patient_id, first_name, last_name, birth_date, start_date) VALUES
    (11, 'Louis', 'Bernard', '1987-09-15', '2017-06-22'),
    (12, 'Emma', 'Morel', '1993-12-02', '2020-03-11'),
    (13, 'Nathan', 'Simon', '1985-04-08', '2018-10-29'),
    (14, 'Camille', 'Rousseau', '1977-08-21', '2016-11-14'),
    (15, 'Léa', 'Lambert', '1992-05-17', '2019-09-05'),
    (16, 'Paul', 'Dupont', '1983-07-23', '2022-01-07'),
    (17, 'Juliette', 'Marchand', '1998-11-30', '2023-06-18'),
    (18, 'Arthur', 'Benoît', '1996-06-15', '2021-07-23'),
    (19, 'Chloé', 'Collet', '1989-03-25', '2022-10-30'),
    (20, 'Thomas', 'Giraud', '1994-01-10', '2020-12-17');

-- Insert new analyses for Region 1
INSERT INTO region1_raw.analyses (analysis_id, patient_id, analysis_date, blood_group, status) VALUES
    (11, 11, '2023-02-10', 'O+', 'validé'),
    (12, 12, '2022-09-25', 'A-', 'en attente'),
    (13, 13, '2023-05-14', 'B+', 'rejeté'),
    (14, 14, '2023-07-30', 'AB-', 'validé'),
    (15, 15, '2022-12-15', 'O-', 'validé'),
    (16, 16, '2023-03-08', 'A+', 'en attente'),
    (17, 17, '2023-06-10', 'B-', 'validé'),
    (18, 18, '2023-10-22', 'AB+', 'rejeté'),
    (19, 19, '2023-09-05', 'O+', 'validé'),
    (20, 20, '2023-11-20', 'A-', 'validé'),
    (21, 4, '2024-07-30', 'AB-', 'validé'),
    (22, 5, '2024-12-15', 'O-', 'validé');

-- Insert new deliverances for Region 1
INSERT INTO region1_raw.delivrances (delivrance_id, patient_id, delivrance_date, blood_type, volume_ml) VALUES
    (11, 11, '2023-04-12', 'O+', 450),
    (12, 14, '2023-08-15', 'AB-', 500),
    (13, 15, '2023-12-01', 'O-', 400),
    (14, 17, '2023-07-11', 'B-', 475),
    (15, 19, '2023-10-03', 'O+', 500),
    (16, 4, '2024-07-20', 'O+', 475),
    (17, 5, '2024-10-03', 'AB-', 500);

-- Insert new patients for Region 2
INSERT INTO region2_raw.patients (patient_id, first_name, last_name, birth_date, start_date) VALUES
    (11, 'Louis', 'Bernard', '1987-09-15', '2018-05-17'),
    (12, 'Emma', 'Morel', '1993-12-02', '2021-07-20'),
    (13, 'Nathan', 'Simon', '1985-04-08', '2019-11-11'),
    (14, 'Camille', 'Rousseau', '1977-08-21', '2017-06-28'),
    (15, 'Léa', 'Lambert', '1992-05-17', '2020-10-15'),
    (16, 'Paul', 'Dupont', '1983-07-23', '2023-02-06'),
    (17, 'Juliette', 'Marchand', '1998-11-30', '2022-08-12'),
    (18, 'Arthur', 'Benoît', '1996-06-15', '2021-12-09'),
    (19, 'Chloé', 'Collet', '1989-03-25', '2023-05-02'),
    (20, 'Thomas', 'Giraud', '1994-01-10', '2022-04-23');

-- Insert new analyses for Region 2
INSERT INTO region2_raw.analyses (analysis_id, patient_id, analysis_date, blood_group, status) VALUES
    (11, 11, '2023-01-19', 'Oplus', 'validé'),
    (12, 12, '2023-06-24', 'Aminus', 'en attente'),
    (13, 13, '2023-05-08', 'Bplus', 'rejeté'),
    (14, 14, '2023-08-20', 'ABminus', 'validé'),
    (15, 15, '2022-11-30', 'Ominus', 'validé'),
    (16, 16, '2023-02-14', 'Aplus', 'en attente'),
    (17, 17, '2023-05-15', 'Bminus', 'validé'),
    (18, 18, '2023-09-18', 'ABplus', 'rejeté'),
    (19, 19, '2023-08-11', 'Oplus', 'validé'),
    (20, 20, '2023-10-23', 'Aminus', 'validé'),
    (21, 4, '2024-07-30', 'Bminus', 'validé'),
    (22, 5, '2024-12-15', 'Oplus', 'validé');

-- Insert new deliverances for Region 2
INSERT INTO region2_raw.delivrances (delivrance_id, patient_id, delivrance_date, blood_type, volume_L) VALUES
    (11, 11, '2023-02-20', 'Oplus', 0.45),
    (12, 14, '2023-09-01', 'ABminus', 0.50),
    (13, 15, '2023-12-10', 'Ominus', 0.40),
    (14, 17, '2023-06-30', 'Bminus', 0.475),
    (15, 19, '2023-10-25', 'Oplus', 0.500),
    (16, 4, '2024-07-20', 'Ominus', 0.475),
    (17, 5, '2024-10-03', 'ABplus', 0.5);

-- Change birth date for non duplicate 
UPDATE region1_raw.patients 
SET birth_date = '1985-01-17' 
WHERE first_name = 'Alice' AND last_name = 'Dupont' AND start_date = '2020-01-01';

UPDATE region1_raw.patients 
SET birth_date = '1992-06-02' 
WHERE first_name = 'Bob' AND last_name = 'Martin' AND start_date = '2018-05-12';

UPDATE region2_raw.delivrances 
SET volume_L = 0.99 
WHERE delivrance_id = 5;