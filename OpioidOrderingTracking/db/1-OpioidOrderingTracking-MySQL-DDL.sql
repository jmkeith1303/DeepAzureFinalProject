create database opiod_ordering_tracking;

create table opiod_ordering_tracking.zipcodes (
  zip_code varchar(9) not null primary key,
  place_name varchar(100) not  null,
  state_name varchar(50) not null,
  state_code varchar(2) not null,
  county varchar(100),
  latitude varchar(50) not null,
  longitude varchar(50) not null
);

create table opiod_ordering_tracking.ndc_product (
  product_id varchar(100) not null primary key,
  fda_ndc varchar(20) not null,
  nonproprietary_name varchar(256),
  labeler_name varchar(256),
  pharmaceutical_classes varchar(512),
  dea_schedule varchar(10) not null,
  is_opiod char(1) not null default 'N'
);

create table opiod_ordering_tracking.ndc_package (
  ndc_11digit varchar(11) not null primary key,
  product_id varchar(100) not null,
  fda_ndc varchar(20) not null,
  ndc_package_code varchar(20) not null,
  package_description varchar(512)
);


create table opiod_ordering_tracking.account (
  isa_sender_id varchar(15) not null,
  account_number varchar(30) not null,
  state_code char(2) not null,
  zip_code varchar(9) not null
);

create unique index account_and_isa on opiod_ordering_tracking.account
  (isa_sender_id, account_number);

create table opiod_ordering_tracking.ordering_history (
  ordering_history_num BIGINT NOT NULL AUTO_INCREMENT,
  order_date date not null,
  isa_sender_id varchar(15) not null,
  account_number varchar(30) not null,
  state_code char(2) not null,
  zip_code varchar(9) not null,
  purchase_order_number varchar(50) not null,
  ordered_ndc varchar(11) not null,
  ordered_ndc_dea_schedule varchar(10) not null,
  ordered_ndc_is_opiod char(1) not null,
  ordered_item_number varchar(50),
  ordered_quantity integer(9) not null,
  shipped_ndc varchar(11),
  shipped_ndc_dea_schedule varchar(10) not null,
  shipped_ndc_is_opiod char(1) not null,
  shipped_item_number varchar(50),
  shipped_quantity integer(9) not null,
  date_added date,
  PRIMARY KEY (ordering_history_num)
);

