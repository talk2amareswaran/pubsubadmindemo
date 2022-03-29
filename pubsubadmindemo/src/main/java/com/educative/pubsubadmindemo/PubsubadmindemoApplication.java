package com.educative.pubsubadmindemo;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.google.cloud.spring.pubsub.PubSubAdmin;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;

@SpringBootApplication
public class PubsubadmindemoApplication implements CommandLineRunner {

	@Autowired
	private PubSubAdmin pubSubAdmin;

	public static void main(String[] args) {
		SpringApplication.run(PubsubadmindemoApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {

		String topicName = "order";
		String unknownTopicName = "package";
		String subscriptionName = "order-packaging";
		String anotherSubscriptionName = "order-shipping";
		String unknownSubscriptionName = "order-notification";
		String pushDeliveryURL = "https://test-project.appspot.com/push";

		createNewTopic(topicName);
		getTopic(topicName);
		getTopic(unknownTopicName);
		listTopics(); 
		createSubscription(subscriptionName, topicName);
		setPushDeliveryMethodSubscription(anotherSubscriptionName, topicName, pushDeliveryURL);
		getSubscription(subscriptionName);
		getSubscription(anotherSubscriptionName);
		getSubscription(unknownSubscriptionName);
		listSubscriptions();
		deleteSubscription(subscriptionName);
		deleteSubscription(anotherSubscriptionName); 
		deleteTopic(topicName);

	}

	private void getSubscription(String subscriptionName) {
		System.out.println("Fetching a subscription: ["+subscriptionName+"]");
		Subscription subscription = pubSubAdmin.getSubscription(subscriptionName);
		if(subscription!=null) {
			System.out.println("Subscription ["+subscriptionName+"] exists");
			return;
		}
		System.out.println("Subscription ["+subscriptionName+"] doesn't exists");
		
	}

	private void getTopic(String topicName) {
		System.out.println("Fetching a topic: ["+topicName+"]");
		Topic topic = pubSubAdmin.getTopic(topicName);
		if(topic!=null) {
			System.out.println("Topic ["+topicName+"] exists");
			return;
		}
		System.out.println("Topic ["+topicName+"] doesn't exists");
	}

	private void deleteSubscription(String subscriptionName) {
		System.out.println("Deleting subscription: ["+subscriptionName+"]");
		pubSubAdmin.deleteSubscription(subscriptionName);
		System.out.println("Subscription ["+subscriptionName+"] deleted successfully");
	}

	private void setPushDeliveryMethodSubscription(String subscriptionName, String topicName, String pushDeliveryURL) {
		System.out.println("Creating a new subscription: ["+subscriptionName+"] for the topic: ["+topicName+"] and delivery Method PUSH URL is: ["+pushDeliveryURL+"]");
		pubSubAdmin.createSubscription(subscriptionName, topicName, pushDeliveryURL);
		System.out.println("Subscription ["+subscriptionName+"] created successfully");
	}

	private void createNewTopic(String topicName) {
		System.out.println("Creating a new topic: ["+topicName+"]");
		pubSubAdmin.createTopic(topicName);
		System.out.println("Topic ["+topicName+"] created successfully");
	}

	private void listTopics() {
		List<Topic> topicsList = pubSubAdmin.listTopics();
		for (Topic topic : topicsList) {
			System.out.println("Available Topic Name(s): [" + topic.getName()+"]");
		}

	}

	private void createSubscription(String subscriptionName, String topicName) {
		System.out.println("Creating a new subscription: ["+subscriptionName+"] for the topic: ["+topicName+"]");
		pubSubAdmin.createSubscription(subscriptionName, topicName);
		System.out.println("Subscription ["+subscriptionName+"] created successfully");
	}

	private void listSubscriptions() {
		List<Subscription> subscriptionsList = pubSubAdmin.listSubscriptions();
		for (Subscription subscription : subscriptionsList) {
			System.out.println("Available Subscription Name(s):" + subscription.getName());
		}
	}

	private void deleteTopic(String topicName) {
		System.out.println("Deleting topic: ["+topicName+"]");
		pubSubAdmin.deleteTopic(topicName);
		System.out.println("Deleted topic: ["+topicName+"]");
	}
	

}
