/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.var.cfg.security;

import static com.activeviam.var.cfg.security.SecurityConfig.ROLE_ADMIN;
import static com.activeviam.var.cfg.security.SecurityConfig.ROLE_CS_ROOT;
import static com.activeviam.var.cfg.security.SecurityConfig.ROLE_USER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.password.PasswordEncoder;

/**
 * User details service holding a collection of users in-memory. Passwords are encoded with a
 * password encoder that must be provided.
 *
 * @author ActiveViam
 */
public class InMemoryUserDetails implements UserDetailsService {

	/**
	 * Password encoder
	 */
	private final PasswordEncoder encoder;

	/**
	 * Users
	 */
	private final List<ActivePivotUser> users;

	public InMemoryUserDetails(PasswordEncoder encoder) {
		this.encoder = encoder;
		this.users = new ArrayList<>();
		loadUsers();
	}

	protected void loadUsers() {
		// Hardcoded list of users, for testing only
		this.users.add(new ActivePivotUser("admin",
				encoder.encode("admin"),
				ROLE_USER,
				ROLE_ADMIN,
				ROLE_CS_ROOT));
		this.users.add(new ActivePivotUser("user", encoder.encode("user"), ROLE_USER));
	}

	@Override
	public UserDetails loadUserByUsername(String username) throws UsernameNotFoundException {

		for (ActivePivotUser user : users) {
			if (user.getUsername().equals(username)) {

				List<GrantedAuthority> grantedAuthorities =
						user.getRoles().stream()
								.map(role -> new SimpleGrantedAuthority(role))
								.collect(Collectors.toList());

				// return a spring security user
				return new User(user.getUsername(), user.getPassword(), grantedAuthorities);
			}
		}

		// If user not found. Throw this exception.
		throw new UsernameNotFoundException("Username: " + username + " not found");
	}

	/**
	 * In-Memory representation of a user.
	 *
	 * @author ActiveViam
	 */
	private static class ActivePivotUser {

		private String username, password;

		/**
		 * Comma separated list of roles
		 */
		private List<String> roles;

		public ActivePivotUser(String username, String password, List<String> roles) {
			this.username = username;
			this.password = password;
			this.roles = roles;
		}

		public ActivePivotUser(String username, String password, String... roles) {
			this(username, password, Arrays.asList(roles));
		}

		public String getUsername() {
			return username;
		}

		public String getPassword() {
			return password;
		}

		public List<String> getRoles() {
			return roles;
		}

	}

}
