INSERT INTO partners (partner_name,partner_status) 
VALUES ('A',True)
ON CONFLICT (partner_name) DO NOTHING;

INSERT INTO partners (partner_name,partner_status) 
VALUES ('B',True)
ON CONFLICT (partner_name) DO NOTHING;


INSERT INTO partners (partner_name,partner_status) 
VALUES ('C',True)
ON CONFLICT (partner_name) DO NOTHING;


INSERT INTO partners (partner_name,partner_status) 
VALUES ('D',False)
ON CONFLICT (partner_name) DO NOTHING;

