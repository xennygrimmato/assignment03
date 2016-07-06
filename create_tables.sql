CREATE TABLE IF NOT EXISTS `cnu2016_vtulsyan`.`order_has_product` (
  `order_id` INT NOT NULL,
  `product_id` INT NOT NULL,
  `quantity` INT NULL DEFAULT 0,
  `buying_cost` DECIMAL(2) NOT NULL,
  `selling_cost` DECIMAL(2) NOT NULL,
  PRIMARY KEY (`order_id`, `product_id`),
  CONSTRAINT `fk_order_has_product_order1`
    FOREIGN KEY (`order_id`)
    REFERENCES `cnu2016_vtulsyan`.`order` (`oid`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_order_has_product_product1`
    FOREIGN KEY (`product_id`)
    REFERENCES `cnu2016_vtulsyan`.`product` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION);
