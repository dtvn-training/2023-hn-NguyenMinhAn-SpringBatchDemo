package com.annm.spring.batch.service.Impl;

import com.annm.spring.batch.entity.Customer;
import com.annm.spring.batch.repository.CustomerRepository;
import com.annm.spring.batch.service.CustomerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class CustomerServiceImpl implements CustomerService {

    @Autowired
    CustomerRepository customerRepository;

    @Override
    public Customer save(Customer customer) {
        Customer customer1 = new Customer();
        customer1.setId(customer.getId());
        customer1.setContactNo(customer.getContactNo());
        customer1.setCountry(customer.getCountry());
        customer1.setDob(customer.getDob());
        customer1.setEmail(customer.getEmail());
        customer1.setGender(customer.getGender());
        customer1.setFirstName(customer.getFirstName());
        customer1.setLastName(customer.getLastName());
        Customer customerSave = customerRepository.save(customer1);
        return customerSave;
    }
}
