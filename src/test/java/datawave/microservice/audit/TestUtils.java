package datawave.microservice.audit;

import datawave.microservice.authorization.user.ProxiedUserDetails;
import datawave.security.authorization.DatawaveUser;
import datawave.security.authorization.SubjectIssuerDNPair;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.function.Executable;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.HttpStatusCodeException;

import java.util.Collection;
import java.util.Collections;

import static datawave.security.authorization.DatawaveUser.UserType.USER;

public class TestUtils {
    
    public static final SubjectIssuerDNPair USER_DN = SubjectIssuerDNPair.of("userDn", "issuerDn");
    
    /**
     * Build ProxiedUserDetails instance with the specified user roles and auths
     */
    public static ProxiedUserDetails userDetails(Collection<String> assignedRoles, Collection<String> assignedAuths) {
        DatawaveUser dwUser = new DatawaveUser(USER_DN, USER, assignedAuths, assignedRoles, null, System.currentTimeMillis());
        return new ProxiedUserDetails(Collections.singleton(dwUser), dwUser.getCreationTime());
    }
    
    public static <T extends HttpStatusCodeException> void assertHttpException(Class<T> exceptionClass, int statusCode, Executable executable) {
        HttpStatusCodeException thrown = Assertions.assertThrows(exceptionClass, executable);
        Assertions.assertEquals(statusCode, thrown.getRawStatusCode(), "Unexpected HTTP status code");
    }
    
    /**
     * Matcher that can be used for both {@link HttpClientErrorException} and {@link HttpServerErrorException}
     */
    public static class StatusMatcher extends TypeSafeMatcher<HttpStatusCodeException> {
        private int status;
        
        public StatusMatcher(int status) {
            this.status = status;
        }
        
        @Override
        protected boolean matchesSafely(HttpStatusCodeException e) {
            return e.getStatusCode().value() == status;
        }
        
        @Override
        public void describeTo(Description description) {
            description.appendText("expects status code ").appendValue(status);
        }
        
        @Override
        protected void describeMismatchSafely(HttpStatusCodeException e, Description mismatchDescription) {
            mismatchDescription.appendText("was ").appendValue(e.getStatusCode().value());
        }
    }
}
