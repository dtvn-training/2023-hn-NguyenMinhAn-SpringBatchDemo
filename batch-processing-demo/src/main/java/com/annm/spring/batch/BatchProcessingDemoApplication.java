package com.annm.spring.batch;

import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.lang.management.ManagementFactory;
import java.sql.SQLOutput;
import java.util.*;

@SpringBootApplication
public class BatchProcessingDemoApplication {

	public static void main(String[] args) {

		ConfigurableApplicationContext context = SpringApplication.run(BatchProcessingDemoApplication.class, args);

		boolean exit = false;
		Job job1 = context.getBean("runJob", Job.class);
		Job job2 = context.getBean("runJob2", Job.class);
		Job job3 = context.getBean("runJob3", Job.class);
		JobLauncher jobLauncher = context.getBean(JobLauncher.class);

//		while (!exit) {
//			System.out.println("Menu:");
//			System.out.println("1. Run CSV to Database");
//			System.out.println("2. Run Database to CSV");
//			System.out.println("3. Exit");
//
//			System.out.print("Enter your choice: ");
//			String choice = System.getProperty("job1");
//
//			switch (choice) {
//				case "1":
//					runJob(jobLauncher, job1);
//					break;
//				case "2":
//					runJob(jobLauncher, job2);
//					break;
//				case "3":
//					exit = true;
//					break;
//				default:
//					System.out.println("Invalid choice. Please enter a valid option.");
//					break;
//			}
//		}



		String VMArgumentsList[] = ManagementFactory.getRuntimeMXBean().getInputArguments().toArray(new String[0]);
		List<String> JobList = new ArrayList<String>();
		for (String s : VMArgumentsList) {
			if (s.contains("-Djob")){
				int equalIndex = s.indexOf("=");
				s = s.substring(2, equalIndex);
				JobList.add(s);
			}
		}
		for (String s : JobList) {
			String choice = System.getProperty(s);
			switch (choice) {
				case "CSVtoDB":
					runJob(jobLauncher, job1);
					break;
				case "DBtoCSV":
					runJob(jobLauncher, job2);
					break;
				case "BigQueryToDB":
					runJob(jobLauncher, job3);
					break;
				default:
					System.out.println("Invalid choice. Please enter a valid option.");
					break;
			}
		}

		context.close();


	}

//	static String fullVMArguments() {
//		String name = javaVmName();
//		return (contains(name, "Server") ? "-server "
//				: contains(name, "Client") ? "-client " : "")
//				+ joinWithSpace(vmArguments());
//	}
//
//	static List<String> vmArguments() {
//		return ManagementFactory.getRuntimeMXBean().getInputArguments();
//	}
//
//	static boolean contains(String s, String b) {
//		return s != null && s.indexOf(b) >= 0;
//	}
//
//	static String javaVmName() {
//		return System.getProperty("java.vm.name");
//	}
//
//	static String joinWithSpace(Collection<String> c) {
//		return join(" ", c);
//	}
//
//	public static String join(String glue, Iterable<String> strings) {
//		if (strings == null) return "";
//		StringBuilder buf = new StringBuilder();
//		Iterator<String> i = strings.iterator();
//		if (i.hasNext()) {
//			buf.append(i.next());
//			while (i.hasNext())
//				buf.append(glue).append(i.next());
//		}
//		return buf.toString();
//	}

	public static void runJob(JobLauncher jobLauncher, Job job) {
		JobParameters jobParameters = new JobParametersBuilder()
				.addLong("startAt", System.currentTimeMillis()).toJobParameters();
		try {
			jobLauncher.run(job, jobParameters);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
