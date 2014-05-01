/*
 * Copyright (c) 2013 GraphAware
 *
 * This file is part of GraphAware.
 *
 * GraphAware is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details. You should have received a copy of
 * the GNU General Public License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

package com.graphaware.module.timetree;

import com.graphaware.test.integration.IntegrationTest;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static com.graphaware.test.util.TestUtils.get;

/**
 * {@link IntegrationTest} for {@link TimeTree} module and {@link TimeTreeApi}.
 */
public class TimeTreeIntegrationTest extends IntegrationTest {

    @Test
    public void graphAwareApisAreMountedWhenPresentOnClasspath() throws InterruptedException, IOException {
        get("http://localhost:7474/graphaware/timetree/now/", HttpStatus.OK_200);
    }
}
